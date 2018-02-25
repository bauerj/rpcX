import asyncio
from collections import deque
from functools import partial
import json
import logging
import numbers
import time
import traceback

import pytest

from rpcx import *


def test_RPCRequest():
    request = RPCRequest.create_next('method', [1], None, 1)
    assert request.method == 'method'
    assert request.args == [1]
    assert request.request_id == 0
    assert not request.is_notification()
    assert repr(request) == "RPCRequest('method', [1], 0)"
    request = RPCRequest.create_next('foo', {"bar": 1}, None, 1)
    assert request.method == 'foo'
    assert request.args == {"bar": 1}
    assert request.request_id == 1
    assert not request.is_notification()
    assert repr(request) == "RPCRequest('foo', {'bar': 1}, 1)"
    request = RPCRequest('foo', [], None)
    assert request.method == 'foo'
    assert request.args == []
    assert request.request_id is None
    assert request.is_notification()
    assert repr(request) == "RPCRequest('foo', [], None)"
    request = RPCRequest('add', {}, 0)
    assert request.method == 'add'
    assert request.args == {}
    assert request.request_id == 0
    assert not request.is_notification()
    assert repr(request) == "RPCRequest('add', {}, 0)"


def test_RPCResponse():
    response = RPCResponse('result', 1)
    assert response.result == 'result'
    assert response.request_id == 1
    assert repr(response) == "RPCResponse('result', 1)"
    error = RPCError(1, 'message', 1)
    assert error.request_id == 1
    response = RPCResponse(error, 5)
    assert response.result == error
    assert response.request_id == 5
    assert error.request_id == 5
    error_repr = repr(error)
    assert repr(response) == f"RPCResponse({error_repr}, 5)"


def test_RPCError():
    error = RPCError(1, 'foo', 2)
    assert error.code == 1
    assert error.message == 'foo'
    assert error.request_id == 2
    assert repr(error) == "RPCError(1, 'foo', 2)"
    for error in (RPCError(5, 'bra'), RPCError(5, 'bra', None)):
        assert error.code == 5
        assert error.message == 'bra'
        assert error.request_id is None
        assert repr(error) == "RPCError(5, 'bra')"


def test_RPCBatch():
    with pytest.raises(AssertionError):
        RPCBatch([])
    with pytest.raises(AssertionError):
        RPCBatch(x for x in (1, 2))
    with pytest.raises(AssertionError):
        RPCBatch([RPCRequest('method', [], 0), RPCResponse(6, 0)])
    with pytest.raises(AssertionError):
        RPCBatch([RPCError(0, 'message', 0), RPCResponse(6, 0)])

    requests = [RPCRequest('method', [], 0), RPCRequest('t', [1], 1)]
    batch = RPCBatch(requests)
    assert batch.is_request_batch()
    assert len(batch) == len(requests)
    # test iter()
    assert requests == list(batch)
    assert batch.request_ids == {1, 0}
    assert isinstance(batch.request_ids, frozenset)
    assert repr(batch) == ("RPCBatch([RPCRequest('method', [], 0), "
                           "RPCRequest('t', [1], 1)])")

    responses = [RPCResponse(6, 2)]
    batch = RPCBatch(responses)
    assert not batch.is_request_batch()
    assert len(batch) == len(responses)
    # test iter()
    assert responses == list(batch)
    assert batch.request_ids == {2}
    assert isinstance(batch.request_ids, frozenset)
    assert repr(batch) == "RPCBatch([RPCResponse(6, 2)])"


def test_batch_builder():

    # Test timeout
    assert RPCBatchBuilder(None).timeout == 60
    assert RPCBatchBuilder(None, 30).timeout == 30

    batch = None

    def batch_done(result):
        nonlocal batch
        batch = result

    with RPCBatchBuilder(batch_done) as b:
        b.add_notification("method")
        b.add_request(None, "method")
        b.add_request(None, "method", [])
        b.add_notification("method", [])
        b.add_request(None, "method", {})
        b.add_notification("method", [])

    assert len(batch) == 6
    assert len(batch.request_ids) == 3
    low = min(batch.request_ids)
    high = low + len(batch.request_ids)
    assert batch.request_ids == frozenset(range(low, high))

    with pytest.raises(RuntimeError):
        with RPCBatchBuilder(batch_done) as b:
            pass

# RPC processor tests


class MyRPCProcessor(RPCProcessor):

    def __init__(self, protocol=None):
        protocol = protocol or JSONRPCv2
        loop = asyncio.get_event_loop()
        super().__init__(protocol, JobQueue(loop), logger=self)
        self.responses = deque()
        self.debug_messages = []
        self.debug_message_count = 0
        self.error_messages = []
        self.error_message_count = 0

    def process_all(self):
        # 640 seconds is enough for anyone
        self.job_queue.process_some(time.time() + 640)

    def wait(self):
        queue = self.job_queue
        if queue.tasks:
            queue.loop.run_until_complete(queue.wait_for_all())
            assert not queue.tasks

    def send_message(self, message):
        self.responses.append(message)

    def consume_one_response(self):
        while self.responses:
            response = self.responses.popleft()
            if response:
                return response
        return b''

    def consume_responses(self):
        result = [response for response in self.responses if response]
        self.responses.clear()
        return result

    def all_done(self):
        return len(self.job_queue) == 0 and not self.responses

    def all_sync_done(self):
        return len(self.job_queue.jobs) == 0 and not self.responses

    def expect_error(self, code, text, request_id):
        self.process_all()
        message = self.consume_one_response()
        assert message
        item = self.protocol.process_message(message)
        assert isinstance(item, RPCResponse)
        assert item.request_id == request_id
        item = item.result
        assert isinstance(item, RPCError)
        assert item.code == code
        assert text in item.message
        assert item.request_id == request_id
        self.expect_debug_message(text)

    def expect_debug_message(self, text):
        assert any(text in message for message in self.debug_messages)

    def expect_internal_error(self, text, request_id):
        self.process_all()
        message = self.consume_one_response()
        assert message
        item = self.protocol.process_message(message)
        assert isinstance(item, RPCResponse)
        assert item.request_id == request_id
        item = item.result
        assert isinstance(item, RPCError)
        assert item.code == self.protocol.INTERNAL_ERROR
        assert 'internal' in item.message
        assert item.request_id == request_id
        self.expect_error_message(text)

    def expect_error_message(self, text):
        assert any(text in message for message in self.error_messages)

    def expect_response(self, result, request_id):
        self.process_all()
        message = self.consume_one_response()
        response = RPCResponse(result, request_id)
        assert message == self.protocol.response_bytes(response)

    def expect_nothing(self):
        self.process_all()
        message = self.consume_one_response()
        assert message == b''

    def add_item(self, item):
        self.add_message(self.protocol.item_bytes(item))

    def add_items(self, items):
        for item in items:
            self.add_item(item)

    def add_messages(self, messages):
        for message in messages:
            self.add_message(message)

    def debug(self, message, *args, **kwargs):
        # This is to test log_debug is being called
        self.debug_messages.append(message)
        self.debug_messages.extend(args)
        self.debug_message_count += 1
        logging.debug(message, *args, **kwargs)

    def debug_clear(self):
        self.debug_messages.clear()
        self.debug_message_count = 0

    def exception(self, message, *args, **kwargs):
        self.error_messages.append(message)
        self.error_messages.extend(args)
        self.error_messages.append(traceback.format_exc())
        self.error_message_count += 1
        logging.exception(message, *args, **kwargs)

    def error_clear(self):
        self.error_messages.clear()
        self.error_message_count = 0

    def notification_handler(self, method):
        if method == 'bad_notification':
            made_bad_notification
        return getattr(self, f'on_{method}', None)

    def request_handler(self, method):
        if method == 'bad_request':
            made_bad_request
        if method == 'partial_add_async':
            return partial(self.on_add_async, 100)
        return getattr(self, f'on_{method}', None)

    def on_echo(self, arg):
        return arg

    def on_notify(self, x, y, z=0):
        self.x = x
        self.y = y
        self.z = z

    def on_raise(self):
        return something

    async def on_raise_async(self):
        return anything

    def on_add(self, x, y=4, z=2):
        values = (x, y, z)
        if any(not isinstance(value, numbers.Number) for value in values):
            raise RPCError(-1, 'all values must be numbers')
        return sum(values)

    async def on_add_async(self, x, y=4, z=2):
        return self.on_add(x, y, z)

    # Special function signatures
    def on_add_many(self, first, second=0, *values):
        values += (first, second)
        if any(not isinstance(value, numbers.Number) for value in values):
            raise RPCError(-1, 'all values must be numbers')
        return sum(values)

    # Built-in; 2 positional args, 1 optional 3rd named arg
    on_pow = pow

    def on_echo_2(self, first, *, second=2):
        return [first, second]

    def on_kwargs(self, start, *kwargs):
        return start + len(kwargs)

    def on_both(self, start=2, *args, **kwargs):
        return start + len(args) * 10 + len(kwargs) * 4

def test_basic():
    rpc = RPCProcessor(None, None)
    assert rpc.request_handler('method') is None
    assert rpc.notification_handler('method') is None
    rpc = MyRPCProcessor()
    # With no messages added, there is nothing to do
    assert rpc.all_done()

# INCOMING REQUESTS


def test_unknown_method():
    rpc = MyRPCProcessor()
    # Test unknown method, for both notification and request
    rpc.add_item(RPCRequest('unk1', ["a"], 5))
    rpc.expect_error(rpc.protocol.METHOD_NOT_FOUND, "unk1", 5)
    rpc.add_item(RPCRequest('unk2', ["a"], None))
    rpc.expect_nothing()
    assert rpc.all_done()


def test_too_many_or_few_array_args():
    rpc = MyRPCProcessor()
    # Test too many args, both notification and request
    rpc.add_items([
        RPCRequest('add', [], 0),
        RPCRequest('add', [], None),
        RPCRequest('add', [1, 2, 3, 4], 0),
        RPCRequest('add', [1, 2, 3, 4], None),
        RPCRequest('add_async', [], 0),
        RPCRequest('add_async', [], None),
        RPCRequest('add_async', [1, 2, 3, 4], 0),
        RPCRequest('add_async', [1, 2, 3, 4], None),
    ])
    rpc.expect_error(rpc.protocol.INVALID_ARGS, "0 arguments", 0)
    rpc.expect_error(rpc.protocol.INVALID_ARGS, "4 arguments", 0)
    rpc.expect_error(rpc.protocol.INVALID_ARGS, "0 arguments", 0)
    rpc.expect_error(rpc.protocol.INVALID_ARGS, "4 arguments", 0)
    assert rpc.all_done()


def test_good_args():
    rpc = MyRPCProcessor()
    # Test 2, 1 and no default args
    rpc.add_items([
        RPCRequest('add', [1], 0),
        RPCRequest('add', [1, 2], 0),
        RPCRequest('add', [1, 2, 3], 0),
        RPCRequest('add', [1], None),
        RPCRequest('add_async', [1], 0),
        RPCRequest('add_async', [1, 2], 0),
        RPCRequest('add_async', [1, 2, 3], 0),
        RPCRequest('add_async', [1], None),
    ])
    rpc.expect_response(7, 0)
    rpc.expect_response(5, 0)
    rpc.expect_response(6, 0)
    assert rpc.all_sync_done()
    assert not rpc.all_done()

    rpc.wait()

    # Order may not be reliable here...
    rpc.expect_response(7, 0)
    rpc.expect_response(5, 0)
    rpc.expect_response(6, 0)
    assert rpc.all_done()


def test_named_args_good():
    rpc = MyRPCProcessor()
    # Test 2, 1 and no default args
    rpc.add_items([
        RPCRequest('add', {"x": 1}, 0),
        RPCRequest('add', {"x": 1, "y": 2}, 0),
        RPCRequest('add', {"x": 1, "y": 2, "z": 3}, 0),
        RPCRequest('add', {"x": 1, "z": 8}, 0),
        RPCRequest('add', {"x": 1}, None),
        RPCRequest('add_async', {"x": 1}, 0),
        RPCRequest('add_async', {"x": 1, "y": 2}, 0),
        RPCRequest('add_async', {"x": 1, "y": 2, "z": 3}, "a"),
        RPCRequest('add_async', {"x": 1, "z": 8}, 0),
        RPCRequest('add_async', {"x": 1}, None),
    ])

    rpc.expect_response(7, 0)
    rpc.expect_response(5, 0)
    rpc.expect_response(6, 0)
    rpc.expect_response(13, 0)
    assert rpc.all_sync_done()
    assert not rpc.all_done()

    rpc.wait()

    # Order may not be reliable here...
    rpc.expect_response(7, 0)
    rpc.expect_response(5, 0)
    rpc.expect_response(6, "a")
    rpc.expect_response(13, 0)
    assert rpc.all_done()


def test_named_args_bad():
    rpc = MyRPCProcessor()
    # Test 2, 1 and no default args
    for method in ('add', 'add_async'):
        rpc.add_items([
            # Bad names
            RPCRequest(method, {"x": 1, "t": 1, "u": 3}, 0),
            RPCRequest(method, {"x": 1, "t": 2}, 0),
            RPCRequest(method, {"x": 1, "t": 2}, None),
            # x is required
            RPCRequest(method, {}, 0),
            RPCRequest(method, {"y": 3}, 0),
            RPCRequest(method, {"y": 3, "z": 4}, 0),
            RPCRequest(method, {"y": 3}, None),
        ])

    for method in ('add', 'add_async'):
        rpc.expect_error(rpc.protocol.INVALID_ARGS, 'parameters "t", "u"', 0)
        rpc.expect_error(rpc.protocol.INVALID_ARGS, 'parameter "t"', 0)
        rpc.expect_error(rpc.protocol.INVALID_ARGS, 'parameter "x"', 0)
        rpc.expect_error(rpc.protocol.INVALID_ARGS, 'parameter "x"', 0)
        rpc.expect_error(rpc.protocol.INVALID_ARGS, 'parameter "x"', 0)
    assert rpc.all_done()

    # Test plural
    rpc.add_item(RPCRequest('notify', {}, 0))
    rpc.expect_error(rpc.protocol.INVALID_ARGS, 'parameters "x", "y"', 0)
    assert rpc.all_done()


def test_handler_that_raises():
    rpc = MyRPCProcessor()

    rpc.add_items([
        RPCRequest('raise', [], 0),
        RPCRequest('raise', [], None),
        RPCRequest('raise_async', [], 0),
        RPCRequest('raise_async', [], None),
    ])

    rpc.expect_internal_error("something", 0)
    assert rpc.all_sync_done()
    assert not rpc.all_done()

    rpc.wait()

    rpc.expect_internal_error("anything", 0)
    assert rpc.all_done()


def test_bad_handler_lookup():
    rpc = MyRPCProcessor()

    rpc.add_items([
        RPCRequest('bad_request', [], 0),
        RPCRequest('bad_notification', [], None),
    ])

    rpc.expect_internal_error("made_bad_request", 0)
    rpc.expect_error_message("made_bad_notification")
    assert rpc.all_done()


def test_partial_async():
    rpc = MyRPCProcessor()

    rpc.add_items([
        RPCRequest('partial_add_async', [10, 15], 3),
    ])

    rpc.wait()
    rpc.expect_response(125, 3)
    assert rpc.all_done()


def test_erroneous_request():
    rpc = MyRPCProcessor()
    rpc.add_messages([
        b'\xff',
        b'{"req',
        b'{"method": 2, "id": 1, "jsonrpc": "2.0"}',
    ])

    rpc.expect_error(rpc.protocol.PARSE_ERROR, 'decode', None)
    rpc.expect_error(rpc.protocol.PARSE_ERROR, 'JSON', None)
    rpc.expect_error(rpc.protocol.METHOD_NOT_FOUND, 'string', 1)
    assert rpc.all_done()


def test_request_round_trip():
    '''Round trip test - we send binary requests to ourselves, process the
    requests, send binary responses in response also to ourselves, and then
    process the results.

    This tests request and response serialization, and also that request
    handlers are invoked when a response is received.

    The tests cover a range of valid and invalid requests, and problem
    triggers.

    We also insert a fake duplicate response, and a fake response without
    a request ID, to test they are appropriately handled.
    '''
    rpc = MyRPCProcessor()

    handled = []

    def handle_add(request, result):
        assert request.method == 'add'
        if request.args[1] == "a":
            assert isinstance(result, RPCError)
            assert result.code == -1
            assert "numbers" in result.message
            handled.append('add_bad')
        else:
            assert request.args == [1, 5, 10]
            assert result == 16
            handled.append(request.method)

    def handle_add_async(request, result):
        assert request.method == 'add_async'
        if request.args[0] == "b":
            assert isinstance(result, RPCError)
            assert result.code == -1
            assert "numbers" in result.message
            handled.append('add_async_bad')
        else:
            assert request.args == [1, 5, 10]
            assert result == 16
            handled.append(request.method)

    def handle_echo(request, result):
        assert request.method == 'echo'
        assert request.args[0] == result
        handled.append(request.method)

    def handle_bad_echo(request, result):
        assert request.method == 'echo'
        assert not request.args
        assert isinstance(result, RPCError)
        assert result.code == rpc.protocol.INVALID_ARGS
        handled.append('bad_echo')

    def buggy_handler(request, result):
        assert request.method == 'echo'
        assert request.args[0] == result
        handled.append('buggy')
        fail_buggily

    def bad_request_handler(request, result):
        assert request.method == 'bad_request'
        assert isinstance(result, RPCError)
        assert result.code == rpc.protocol.INTERNAL_ERROR
        handled.append(request.method)

    null_handler_request = RPCRequest.create_next('echo', [2], None, 1.0)
    requests = [
        RPCRequest.create_next('add', [1, 5, 10], handle_add, 1.0),
        # Bad type
        RPCRequest.create_next('add', [1, "a", 10], handle_add, 1.0),
        # Test a notification, and a bad one
        RPCRequest.create_next('echo', ["ping"], handle_echo, 1.0),
        RPCRequest('echo', [], None),
        # Test a None response
        RPCRequest.create_next('echo', [None], handle_echo, 1.0),
        # Throw in an async request
        RPCRequest.create_next('add_async', [1, 5, 10], handle_add_async, 1.0),
        RPCRequest.create_next('add_async', ["b"], handle_add_async, 1.0),
        # An invalid request
        RPCRequest.create_next('echo', [], handle_bad_echo, 1.0),
        # test a null handler
        null_handler_request,
        # test a buggy request handler
        RPCRequest.create_next('echo', ["ping"], buggy_handler, 1.0),
        # test a remote bad request getter
        RPCRequest.create_next('bad_request', [], bad_request_handler, 1.0),
    ]

    # Send each request and feed them back into the RPC object as if
    # it were receiving its own messages.
    for request in requests:
        # Send it and fake receiving it
        rpc.send_request(request)
        rpc.add_message(rpc.responses.pop())

    # Now process the queue and the async jobs, generating queued responses
    rpc.process_all()
    rpc.wait()

    # Get the queued responses and send them back to ourselves
    responses = rpc.consume_responses()
    assert rpc.all_done()

    # Did we did get the null handler response - no other way to detect it
    text = f'"id": {null_handler_request.request_id}'.encode()
    assert any(text in response for response in responses)

    for response in responses:
        rpc.add_message(response)

    # Test it was logged.  It should have no ill-effects
    rpc.expect_error_message('fail_buggily')

    assert sorted(handled) == ['add', 'add_async', 'add_async_bad', 'add_bad',
                               'bad_echo', 'bad_request', 'buggy', 'echo',
                               'echo']

    # Responses are handled synchronously so no process_all() is needed
    assert rpc.all_done()


def test_bad_reponses():
    rpc = MyRPCProcessor()
    handled = []

    def handle_add(request, result):
        assert request.method == 'add'
        assert request.args == [1, 5, 10]
        assert result == 16
        handled.append(request.method)

    requests = [
        RPCRequest.create_next('add', [1, 5, 10], handle_add, 1.0),
    ]

    # Send each request and feed them back into the RPC object as if
    # it were receiving its own messages.
    for request in requests:
        # Send it and fake receiving it
        rpc.send_request(request)
        rpc.add_message(rpc.responses.pop())

    # Now process the queue and the async jobs, generating queued responses
    rpc.process_all()

    # Get the queued responses and send them back to ourselves
    response, = rpc.consume_responses()
    assert rpc.all_done()

    # Send response twice.
    rpc.add_message(response)
    assert handled == ['add']
    assert not rpc.debug_messages
    rpc.add_message(response)
    # Handler NOT called twice
    assert handled == ['add']
    rpc.expect_debug_message('response to unsent request')
    rpc.debug_clear()
    # Send a response with no ID
    rpc.add_message(rpc.protocol.response_bytes(RPCResponse(6, None)))
    rpc.expect_debug_message('missing id')

    # Responses are handled synchronously so no process_all() is needed
    assert rpc.all_done()


def test_timeouts():
    rpc = MyRPCProcessor()
    handled = []

    def handler(request, result):
        assert request.method == 'add'
        if isinstance(result, RPCError):
            assert result.code == rpc.protocol.TIMEOUT_ERROR
            assert result.message == 'request timed out'
            handled.append('timeout')
        else:
            assert result == sum(request.args)
            handled.append('add')

    def buggy_handler(request, result):
        assert result.code == rpc.protocol.TIMEOUT_ERROR
        assert result.message == 'request timed out'
        handled.append('buggy')
        fail_buggily

    sleep_time = 0.1
    requests = [
        # This one should succeed
        RPCRequest.create_next('add', [1, 5, 5], handler, 1.0),
        # This one should time-out
        RPCRequest.create_next('add', [1, 0, 0], handler, sleep_time),
    ]

    # Send each request except the last
    messages = []
    for request in requests:
        # Send it, record for later replay
        rpc.send_request(request)
        messages.append(rpc.responses.pop())

    # Sleep a bit
    time.sleep(sleep_time)

    # Time out expired requests
    assert not rpc.debug_messages
    rpc.timeout_requests()
    rpc.expect_debug_message('timed out')
    assert handled == ['timeout']
    assert rpc.debug_message_count == 1
    assert not rpc.error_messages
    rpc.debug_clear()

    # Send another request with a buggy handler in timed-out state
    # This one has a buggy handler; we are testing its exception is
    # properly handled
    request = RPCRequest.create_next('echo', "satoshi", buggy_handler, 0.0)
    rpc.send_request(request)
    messages.append(rpc.responses.pop())
    rpc.timeout_requests()
    assert rpc.debug_message_count == 1
    rpc.expect_debug_message('timed out')
    rpc.expect_error_message('fail_buggily')
    assert handled == ['timeout', 'buggy']

    # "Receive" the responses for all requests
    for message in messages:
        rpc.add_message(message)

    # Now process the queue and the async jobs, generating queued responses
    # Get the queued responses and send them back to ourselves
    rpc.process_all()
    for response in rpc.consume_responses():
        rpc.add_message(response)
    assert handled == ['timeout', 'buggy', 'add']
    assert rpc.all_done()

def test_batch_round_trip():
    rpc = MyRPCProcessor()
    handled = []
    batch_message = None

    def handle_add(request, result):
        assert request.method in ('add', 'add_async')
        assert request.args == [1, 5, 10]
        assert result == 16
        handled.append(request.method)

    def handle_echo(request, result):
        assert request.method == 'echo'
        assert result == request.args[0]
        handled.append(request.method)

    def handle_bad_echo(request, result):
        assert request.method == 'echo'
        assert not request.args
        assert isinstance(result, RPCError)
        assert result.code == rpc.protocol.INVALID_ARGS
        handled.append('bad_echo')

    def buggy_handler(request, result):
        handled.append('buggy')
        fail_buggily

    def send_and_receive(batch):
        nonlocal batch_message
        # Send the batch
        rpc.send_batch(batch)
        batch_message = rpc.responses.pop()

    with RPCBatchBuilder(send_and_receive) as b:
        b.add_request(handle_add, 'add', [1, 5, 10])
        b.add_request(handle_add, 'add_async', [1, 5, 10])
        b.add_request(handle_bad_echo, 'echo', [])    # An erroneous request
        b.add_request(buggy_handler, 'echo', ["ping"])
        b.add_notification('add')   # Erroneous; gets swallowed anyway
        b.add_request(handle_echo, 'echo', ["ping"])

    assert not rpc.debug_messages
    assert not rpc.error_messages
    # Fake receiving it.  This processes the request and sees invalid
    # requests, and creates jobs to process the valid requests
    rpc.add_message(batch_message)
    assert not rpc.all_done()
    assert rpc.debug_message_count == 2  # Both notification and request
    assert not rpc.error_messages
    rpc.debug_clear()

    # Now process the request jobs, generating queued response messages
    rpc.process_all()
    rpc.wait()
    assert not rpc.debug_messages
    assert not rpc.error_messages

    # Get the batch response and send it back to ourselves
    response, = rpc.consume_responses()
    assert rpc.all_done()

    # Process the batch response
    rpc.add_message(response)
    assert rpc.all_done()

    assert sorted(handled) == ['add', 'add_async', 'bad_echo',
                               'buggy', 'echo']

    # Test it was logged.  It should have no ill-effects
    rpc.expect_error_message('fail_buggily')
    assert rpc.error_message_count == 1
    assert rpc.debug_message_count == 1  # Only request


def test_some_invalid_requests():
    rpc = MyRPCProcessor()

    # First message is good.  2nd misses "jsonrpc" and also has a
    # non-string method.
    batch_message = (
        b'[{"jsonrpc": "2.0", "method": "add", "params": [1]}, '
        b'{"method": 2}]'
    )
    rpc.add_message(batch_message)

    # Now process the request jobs, generating queued response messages
    rpc.process_all()

    # Test a single non-empty response was created
    response, = rpc.consume_responses()

    # There is no response!
    assert rpc.all_done()


def test_all_notification_batch():
    rpc = MyRPCProcessor()

    def send_and_receive(batch):
        rpc.send_batch(batch)
        # Fake reeiving it
        rpc.add_message(rpc.responses.pop())

    with RPCBatchBuilder(send_and_receive) as b:
        b.add_notification('echo', ["ping"])
        b.add_notification('add')   # Erroneous; gets swallowed anyway

    # Now process the request jobs, generating queued response messages
    rpc.process_all()

    # There is no response!
    assert rpc.all_done()


def test_batch_response_bad():
    rpc = MyRPCProcessor()
    handled = []
    request_ids = None
    batch_message = None

    def handler(request, result):
        handled.append(request.method)

    def send_and_receive(batch):
        nonlocal request_ids, batch_message
        request_ids = list(batch.request_ids)
        rpc.send_batch(batch)
        batch_message = rpc.responses.pop()

    def send_batch():
        with RPCBatchBuilder(send_and_receive) as b:
            b.add_request(handler, 'add', [1, 5, 10])
            b.add_request(handler, 'echo', ["ping"])

    send_batch()
    # Fake receiving it
    rpc.add_message(batch_message)

    # Now process the request jobs, generating queued response messages
    rpc.process_all()
    assert not rpc.debug_messages
    assert not rpc.error_messages

    # Before sending a good response, send a batch response with a good ID
    # and a bad ID
    responses = [
        RPCResponse(5, -1),
        RPCResponse(6, request_ids[0])
    ]
    parts = [rpc.protocol.response_bytes(response) for response in responses]
    rpc.add_message(rpc.protocol.batch_bytes_from_parts(parts))

    rpc.expect_debug_message('unsent batch request')
    assert not rpc.error_messages
    assert not handled
    rpc.debug_clear()

    # Get the batch response and send it back to ourselves
    response, = rpc.consume_responses()
    assert rpc.all_done()
    rpc.add_message(response)
    assert sorted(handled) == ['add', 'echo']

    # Send it again, check no handlers are called and it's logged
    rpc.debug_clear()
    rpc.add_message(response)
    assert sorted(handled) == ['add', 'echo']
    rpc.expect_debug_message('unsent batch request')

    # Now send the batch again.  Create a response with the correct IDs
    # but an additional response with a None id.
    handled.clear()
    rpc.debug_clear()
    send_batch()
    assert rpc.all_done()

    responses = [RPCResponse(5, request_id) for request_id in request_ids]
    responses.insert(1, RPCResponse(5, None))
    parts = [rpc.protocol.response_bytes(response) for response in responses]
    rpc.add_message(rpc.protocol.batch_bytes_from_parts(parts))

    # Check (only) the None id was logged.  Check the 2 good respones
    # were handled as expected.
    assert not rpc.error_messages
    assert rpc.debug_message_count == 1
    rpc.expect_debug_message('batch response missing id')
    assert sorted(handled) == ['add', 'echo']
    assert rpc.all_done()


def test_batch_timeout():
    rpc = MyRPCProcessor()
    handled = []

    def handler(request, result):
        handled.append(request.method)
        assert isinstance(result, RPCError)
        assert result.code == rpc.protocol.TIMEOUT_ERROR
        assert 'timed out' in result.message
        assert result.request_id == request.request_id

    def send_and_receive(batch):
        # Send the batch
        rpc.send_batch(batch)
        # Fake receiving it
        rpc.add_message(rpc.responses.pop())

    with RPCBatchBuilder(send_and_receive, timeout=0) as b:
        b.add_request(handler, 'add', [1, 5, 10])
        b.add_request(handler, 'echo', ["ping"])

    # Time out expired requests
    rpc.timeout_requests()
    assert sorted(handled) == ['add', 'echo']
    rpc.expect_debug_message('timed out')
    assert rpc.debug_message_count == 2
    assert not rpc.error_messages
    rpc.debug_clear()

    # Now process the request jobs, generating queued response messages
    rpc.process_all()
    assert not rpc.debug_messages
    assert not rpc.error_messages

    # Get the batch response and send it back to ourselves
    response, = rpc.consume_responses()
    assert rpc.all_done()
    rpc.add_message(response)
    assert rpc.all_done()

    rpc.expect_debug_message('timed-out batch request')
    assert sorted(handled) == ['add', 'echo']


def test_cancelled_async_requests():
    rpc = MyRPCProcessor()

    # Simple request
    rpc.add_item(RPCRequest('add_async', [1], 0))
    rpc.job_queue.cancel_all()

    # Cancellation produces no response individual requests
    rpc.wait()
    # This is needed to consume the empty response
    assert not rpc.consume_responses()
    assert rpc.all_done()

    rpc = MyRPCProcessor()
    rpc.add_item(RPCBatch([
        RPCRequest('add_async', [1], 0),
        RPCRequest('add', [1], 2),
    ]))
    assert rpc.job_queue.jobs
    rpc.process_all()
    assert not rpc.job_queue.jobs
    rpc.job_queue.cancel_all()
    rpc.wait()

    # This tests that the batch is sent and not still waiting.
    # Cancellation produces a partial response to the synchronous job
    # Should we improve this?
    response, = rpc.consume_responses()
    assert rpc.all_done()


def test_odd_calls():
    rpc = MyRPCProcessor()
    handled = []

    def expect(answer, request, result):
        if result == answer:
            handled.append(request.method)
        else:
            print("REQUEST: ", request, result)

    def error(text, request, result):
        if isinstance(result, RPCError) and text in result.message:
            handled.append(text)
        else:
            print("REQUEST: ", request, result)

    requests = [
        RPCRequest.create_next('add_many', [],
                               partial(error, 'requires 1'), 1.0),
        RPCRequest.create_next('add_many', [1],
                               partial(expect, 1), 1.0),
        RPCRequest.create_next('add_many', [5, 50, 500],
                               partial(expect, 555), 1.0),
        RPCRequest.create_next('add_many', list(range(10)),
                               partial(expect, 45), 1.0),
        RPCRequest.create_next('add_many', {'first': 1},
                               partial(expect, 1), 1.0),
        RPCRequest.create_next('add_many', {'first': 1, 'second': 10},
                               partial(expect, 11), 1.0),
        RPCRequest.create_next('add_many', {'first': 1, 'values': []},
                               partial(error, 'values'), 1.0),
        RPCRequest.create_next('pow', [2, 3], partial(expect, 8), 1.0),
        RPCRequest.create_next('pow', [2, 3, 5], partial(expect, 3), 1.0),
        RPCRequest.create_next('pow', {"x": 2, "y": 3},
                               partial(error, 'cannot be called'), 1.0),
        RPCRequest.create_next('echo_2', ['ping'],
                               partial(expect, ['ping', 2]), 1.0),
        RPCRequest.create_next('echo_2', ['ping', 'pong'],
                               partial(error, 'at most 1'), 1.0),
        RPCRequest.create_next('echo_2', {'first': 1, 'second': 8},
                               partial(expect, [1, 8]), 1.0),
        RPCRequest.create_next('echo_2', {'first': 1, 'second': 8, '3rd': 1},
                               partial(error, '3rd'), 1.0),
        RPCRequest.create_next('kwargs', [],
                               partial(error, 'requires 1'), 1.0),
        RPCRequest.create_next('kwargs', [1],
                               partial(expect, 1), 1.0),
        RPCRequest.create_next('kwargs', [1, 2],
                               partial(expect, 2), 1.0),
        RPCRequest.create_next('kwargs', {'end': 4},
                               partial(error, "start"), 1.0),
        RPCRequest.create_next('kwargs', {'start': 3},
                               partial(expect, 3), 1.0),
        RPCRequest.create_next('kwargs', {'start': 3, 'end': 1, '3rd': 1},
                               partial(error, '3rd'), 1.0),
        RPCRequest.create_next('both', [],
                               partial(expect, 2), 1.0),
        RPCRequest.create_next('both', [1],
                               partial(expect, 1), 1.0),
        RPCRequest.create_next('both', [5, 2],
                               partial(expect, 15), 1.0),
        RPCRequest.create_next('both', {'end': 4},
                               partial(expect, 6), 1.0),
        RPCRequest.create_next('both', {'start': 3},
                               partial(expect, 3), 1.0),
        RPCRequest.create_next('both', {'start': 3, 'end': 1, '3rd': 1},
                               partial(expect, 11), 1.0),
    ]

    # Send each request and feed them back into the RPC object as if
    # it were receiving its own messages.
    for request in requests:
        rpc.send_request(request)
        rpc.add_message(rpc.responses.pop())

    # Now process the queue and the async jobs, generating queued responses
    rpc.process_all()

    # Get the queued responses and send them back to ourselves
    responses = rpc.consume_responses()
    assert rpc.all_done()

    for response in responses:
        rpc.add_message(response)

    assert len(handled) == len(requests)

    assert rpc.all_done()
