# Copyright (c) 2018, Neil Booth
#
# All rights reserved.
#
# The MIT License (MIT)
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

'''RPC message processing, independent of transport and RPC protocol.'''


from collections import deque
from concurrent.futures import CancelledError
from functools import partial
import logging
import time
import traceback

from .util import is_async_call, signature_info


class RPCRequest(object):

    bytes_method = 'request_bytes'
    _next_id = -1

    @classmethod
    def create_next(cls, method, args, handler, timeout):
        '''Return a request using the next unique request ID.  Set
        handler and timeout attributes - they only exist on outgoing
        non-notification requests.'''
        cls._next_id += 1
        request = cls(method, args, cls._next_id)
        request.handler = handler
        request.timeout = timeout
        return request

    def __init__(self, method, args, request_id):
        # request_id is None if it's a notification
        self.method = method
        self.args = args
        self.request_id = request_id
        if isinstance(method, RPCError):
            method.request_id = request_id

    def __repr__(self):
        return (f'RPCRequest({self.method!r}, {self.args!r}, '
                f'{self.request_id!r})')

    def is_notification(self):
        return self.request_id is None


class RPCResponse(object):

    bytes_method = 'response_bytes'

    def __init__(self, result, request_id):
        # result is an RPCError object if an error was returned
        self.result = result
        self.request_id = request_id
        if isinstance(result, RPCError):
            result.request_id = request_id

    def __repr__(self):
        return f'RPCResponse({self.result!r}, {self.request_id!r})'


class RPCError(Exception):
    '''An RPC error.

    When an RPC error response is received, an object of this type is
    embedded in the RPCResponse object as its "result" and passed to
    the user-defined response handler.

    When protocol.process_message() parses an incoming request (or
    batch item), if it is ill-formed (for example, it cannot be
    parsed, or the method name is not a string),
    protocol.process_message() should return it embedded in an
    RPCRequest object as its "method".  When the request is processed
    the framework will embed it in a RPCResponse object to send over
    the network.

    A request handler can raise it to cause the framework to send an
    error response.
    '''

    bytes_method = 'error_bytes'

    def __init__(self, code, message, request_id=None):
        super().__init__(message, code)
        self.message = message
        self.code = code
        self.request_id = request_id

    def __repr__(self):
        if self.request_id is None:
            return f'RPCError({self.code:d}, {self.message!r})'
        else:
            return (f'RPCError({self.code:d}, {self.message!r}, '
                    f'{self.request_id!r})')


class RPCBatch(object):
    # Represents a request batch or a response batch

    bytes_method = 'batch_bytes'

    def __init__(self, items):
        self.items = items
        assert isinstance(items, list)
        assert items
        assert (all(isinstance(item, RPCRequest) for item in items)
                or all(isinstance(item, RPCResponse) for item in items))
        # A frozenset of all request IDs in the batch ignoring notifications
        self.request_ids = frozenset(item.request_id for item in self
                                     if item.request_id is not None)

    def is_request_batch(self):
        return isinstance(self.items[0], RPCRequest)

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __repr__(self):
        return f'RPCBatch({self.items!r})'


class RPCBatchBuilder(object):

    def __init__(self, on_done, timeout=60):
        self.requests = []
        self.timeout = timeout
        self.on_done = on_done

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            if not self.requests:
                raise RuntimeError('request batch cannot be empty')
            batch = RPCBatch(self.requests)
            batch.timeout = self.timeout
            self.on_done(batch)

    def add_request(self, handler, method, args=[]):
        request = RPCRequest.create_next(method, args, handler, None)
        self.requests.append(request)

    def add_notification(self, method, args=[]):
        request = RPCRequest(method, args, None)
        self.requests.append(request)


class RPCProcessor(object):
    '''Handles RPC message processing.

    Coordinates the processing of incoming and outgoing RPC requests,
    responses and notifications.
    '''

    def __init__(self, protocol, job_queue, logger=None):
        super().__init__()
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.protocol = protocol
        # Synchronous and asynchronous deferred job handler
        self.job_queue = job_queue
        # Sent requests and batch requests awaiting a response
        # For an RPCRequest object the key is its request ID; for a batch
        # it is its frozenset of request IDs
        self.requests = {}
        self.timed_out_ids = set()

    def _evaluate(self, request, func):
        '''Evaluate func in the context of processing a request.

        If the call returns a result, return it.
        If the call raises a CancelledError, return the error.
        If the call raises an RPC error, log debug it and return the error
        with its request_id set to match that of the request.
        If the call raises any other exception, it indicates a bug in
        the software.  Log the exception, and return an RPCError indicating
        an internal error.
        '''
        try:
            return func()
        except CancelledError as error:
            return error
        except RPCError as error:
            error.request_id = request.request_id
            self.logger.debug('error processing request: %s %s',
                              repr(error), repr(request))
            return error
        except Exception:
            self.logger.exception('exception raised processing request: %s',
                                  repr(request))
            return self.protocol.internal_error(request.request_id)

    def _evaluate_and_send(self, request, func, send_func):
        '''Like _evaluate, but convert the result to bytes and pass them
        to send_func.

        send_func is always called because batches need to count completed
        responses.  Id the result is a CancelledError then
        b'' is sent.
        '''
        result = self._evaluate(request, func)
        if isinstance(result, RPCError):
            result_bytes = self.protocol.error_bytes(result)
        elif isinstance(result, CancelledError):
            result_bytes = b''
        else:
            response = RPCResponse(result, request.request_id)
            result_bytes = self.protocol.response_bytes(response)
        send_func(result_bytes)

    def _rpc_call(self, request):
        '''Return a partial function call that calls the RPC function
        to handle the request with the appropriate arguments.

        If the request is bad an RPCError is raised.  Any exceptions
        raised when determining the handler function are passed on.
        '''
        # Raise ill-formed requests here so that they are logged
        method = request.method
        if isinstance(method, RPCError):
            raise method

        # Let through any exceptions raised when determining the handler
        request_id = request.request_id
        if request_id is None:
            handler = self.notification_handler(method)
        else:
            handler = self.request_handler(method)
        if not handler:
            raise self.protocol.method_not_found(f'unknown method "{method}"')

        # We must test for too few and too many arguments.  How
        # depends on whether the arguments were passed as a list or as
        # a dictionary.
        info = signature_info(handler)
        args = request.args
        if isinstance(args, list):
            if len(args) < info.min_args:
                s = '' if len(args) == 1 else 's'
                raise self.protocol.args_error(
                    f'{len(args)} argument{s} passed to method '
                    f'"{method}" but it requires {info.min_args}')
            if info.max_args is not None and len(args) > info.max_args:
                s = '' if len(args) == 1 else 's'
                raise self.protocol.args_error(
                    f'{len(args)} argument{s} passed to method '
                    f'{method} taking at most {info.max_args}')
            return partial(handler, *args)

        # Arguments passed by name
        if info.other_names is None:
            raise self.protocol.args_error(f'method "{method}" cannot '
                                           f'be called with named arguments')

        missing = set(info.required_names).difference(args)
        if missing:
            s = '' if len(missing) == 1 else 's'
            missing = ', '.join(sorted(f'"{name}"' for name in missing))
            raise self.protocol.args_error(f'method "{method}" requires '
                                           f'parameter{s} {missing}')

        if info.other_names is not any:
            excess = set(args).difference(info.required_names)
            excess = excess.difference(info.other_names)
            if excess:
                s = '' if len(excess) == 1 else 's'
                excess = ', '.join(sorted(f'"{name}"' for name in excess))
                raise self.protocol.args_error(f'method "{method}" does not '
                                               f'take parameter{s} {excess}')
        return partial(handler, **args)

    def _process_request(self, request, send_func):
        '''Process a request or notification.

        If it is not a notification, send_func will be called with
        the response bytes, either now or later.
        '''
        # Wrap the call to _rpc_call in _evaluate in order to
        # correctly handle exceptions it might raise.
        rpc_call = self._evaluate(request, partial(self._rpc_call, request))

        if isinstance(rpc_call, RPCError):
            # Always send responses to ill-formed requests
            if request.request_id is not None or isinstance(request.method,
                                                            RPCError):
                send_func(self.protocol.error_bytes(rpc_call))
            return

        # Handling depends on whether the handler is async or not.
        # Notifications just evaluate the RPC call; requests send the result.
        if is_async_call(rpc_call):
            def on_done(task):
                if request.request_id is None:
                    self.evaluate(request, task.result)
                else:
                    self._evaluate_and_send(request, task.result, send_func)
            self.job_queue.add_coroutine(rpc_call(), on_done)
        else:
            if request.request_id is None:
                job = partial(self._evaluate, request, rpc_call)
            else:
                job = partial(self._evaluate_and_send, request, rpc_call,
                              send_func)
            self.job_queue.add_job(job)

    def _process_request_batch(self, batch):
        '''For request batches, queue each request individually except
        that the results must be collected and not sent.  The response
        is only sent when all the individual responses have come in.
        '''
        def on_done(response):
            if response:
                parts.append(response)
            nonlocal remaining
            remaining -= 1
            if not remaining:
                self.send_message(self.protocol.batch_bytes_from_parts(parts))

        parts = []
        remaining = len([item for item in batch
                         if item.request_id is not None or
                         isinstance(item.method, RPCError)])
        for request in batch:
            self._process_request(request, on_done)

    def _handle_request_response(self, request, response):
        # NOTE: response handlers are intended to be quick and so are
        # called synchronously when the response is received
        if isinstance(response.result, RPCError):
            self.logger.debug('request returned errror: %s %s',
                              repr(request), repr(response.result))
        if request.handler:
            try:
                request.handler(request, response.result)
            except Exception:
                self.logger.exception('exception raised in handler for '
                                      'request %s', repr(request))

    def _process_response(self, response):
        request_id = response.request_id
        if request_id is None:
            self.logger.debug('missing id: %s', repr(response))
            return

        request = self.requests.pop(request_id, None)
        if request:
            self._handle_request_response(request, response)
        elif request_id in self.timed_out_ids:
            self.timed_out_ids.remove(request_id)
            self.logger.debug('response to timed-out request: %s',
                              repr(response))
        else:
            self.logger.debug('response to unsent request: %s', repr(response))

    def _process_response_batch(self, batch):
        request_ids = batch.request_ids
        batch_request = self.requests.pop(request_ids, None)
        if batch_request:
            requests_by_id = {item.request_id: item for item in batch_request
                              if item.request_id is not None}
            for response in batch:
                request_id = response.request_id
                if request_id is None:
                    self.logger.debug('batch response missing id: %s',
                                      repr(response))
                else:
                    request = requests_by_id[request_id]
                    self._handle_request_response(request, response)
        elif request_ids in self.timed_out_ids:
            self.timed_out_ids.remove(request_ids)
            self.logger.debug('response to timed-out batch request')
        else:
            self.logger.debug('response to unsent batch request: %s',
                              repr(batch))

    # External API - methods for use by a session layer
    def add_message(self, message):
        '''Analyse an incoming message and queue it for processing.

        Any response message will be sent to send_msessage.  This can
        happen before or after this function returns.
        '''
        item = self.protocol.process_message(message)
        if isinstance(item, RPCRequest):
            self._process_request(item, self.send_message)
        elif isinstance(item, RPCResponse):
            self._process_response(item)
        else:
            assert isinstance(item, RPCBatch)
            if item.is_request_batch():
                self._process_request_batch(item)
            else:
                self._process_response_batch(item)

    def send_request(self, request):
        '''Call when sending a request.  If it is not a notification
        then save it so that an incoming response, and the timeout, can
        be handled.

        Returns the binary request.'''
        if not request.is_notification():
            request.timeout += time.time()
            self.requests[request.request_id] = request
        self.send_message(self.protocol.request_bytes(request))

    def send_batch(self, batch):
        '''Call when sending a batch request.  Unless it is all notifications,
        save the batch so that an incoming batch response, and
        timeout, can be handled.
        '''
        if batch.request_ids:
            batch.timeout += time.time()
            self.requests[batch.request_ids] = batch
        self.send_message(self.protocol.batch_bytes(batch))

    def timeout_requests(self, current_time=None):
        '''Timeout any requests that have reached their timeout.'''
        if current_time is None:
            current_time = time.time()
        timed_out_ids = [request_id
                         for request_id, request in self.requests.items()
                         if request.timeout < current_time]

        def timeout_request(request):
            error = self.protocol.timeout_error(request.request_id)
            response = RPCResponse(error, request.request_id)
            self._handle_request_response(request, response)

        for request_id in timed_out_ids:
            request = self.requests.pop(request_id)
            if isinstance(request, RPCBatch):
                for item in request:
                    timeout_request(item)
            else:
                timeout_request(request)
            self.timed_out_ids.add(request_id)

    # Methods to be overridden in a derived class
    def send_message(self, message):
        '''Called when there is a message ready to send over the network.  The
        message is unframed.  It might be empty, in which case it
        should be ignored.

        The derived class may want to queue several messages and send
        them as a batch, or delay / throttle the sends in some way.
        '''

    def notification_handler(self, method):
        '''Return the handler for the given notification.

        The handler can be synchronous or asynchronous.  The return
        value is ignored.'''
        return None

    def request_handler(self, method):
        '''Return the handler for the given request method.

        The handler can be synchronous or asynchronous.  The return value
        is sent as an RPC response.'''
        return None
