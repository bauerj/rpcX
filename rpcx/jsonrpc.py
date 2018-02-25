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

'''Classes for JSONRPC versions 1.0 and 2.0, and a loose interpretation.'''

import json
import logging
import numbers
import traceback

from .rpc import RPCError, RPCBatch, RPCRequest, RPCResponse


class JSONRPC(object):
    '''Abstract base class that interprets and constructs JSON RPC messages.'''

    # Error codes.  See http://www.jsonrpc.org/specification
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_ARGS = -32602
    INTERNAL_ERROR = -32603
    # Codes specific to this library
    ERROR_CODE_UNAVAILABLE = -100
    TIMEOUT_ERROR = -101

    # Can be overridden by client application
    logger = logging.getLogger('JSONRPC')

    # Can be overridden by derived classes
    allow_batches = True

    @classmethod
    def _message_id(cls, message, require_id):
        '''Validate the message is a dictionary and return its ID.

        Raise an error if the message is invalid or the ID is of an
        invalid type.  If it has no ID, raise an error if require_id
        is True, otherwise return None.
        '''
        raise NotImplementedError

    @classmethod
    def _validate_message(cls, message):
        '''Validate other parts of the message other than those
        done in _message_id.'''
        pass

    @classmethod
    def _request_args(cls, request):
        '''Validate the existence and type of the arguments passed
        in the request dictionary.'''
        raise NotImplementedError

    @classmethod
    def _process_batch(cls, batch):
        if not batch:
            return RPCRequest(cls.invalid_request('empty batches are invalid'),
                              None, None)

        if any(isinstance(item, dict) and 'method' in item for item in batch):
            processor = cls._process_request
        else:
            processor = cls._process_response

        return RPCBatch([processor(item) for item in batch])

    @classmethod
    def _process_request(cls, item):
        request_id = None
        try:
            request_id = cls._message_id(item, False)
            cls._validate_message(item)
            method = item.get('method')
            if not isinstance(method, str):
                raise RPCError(cls.METHOD_NOT_FOUND,
                               'request method must be a string')
            return RPCRequest(method, cls._request_args(item), request_id)
        except RPCError as error:
            return RPCRequest(error, None, request_id)
        except Exception:
            cls.logger.exception(f'error processing request {item!r}')
            return RPCRequest(cls.internal_error(request_id), None,
                              request_id)

    @classmethod
    def _process_response(cls, item):
        request_id = None
        try:
            request_id = cls._message_id(item, True)
            cls._validate_message(item)
            return RPCResponse(cls.response_result(item), request_id)
        except RPCError as error:
            return RPCResponse(error, request_id)
        except Exception:
            cls.logger.exception(f'error processing response {item!r}')
            return RPCResponse(cls.internal_error(request_id),
                               request_id)

    @classmethod
    def _message_to_item(cls, message):
        '''Returns a Python object or an RPCError.'''
        try:
            return json.loads(message.decode())
        except UnicodeDecodeError as e:
            return RPCError(cls.PARSE_ERROR,
                            f'messages must be encoded in UTF-8: {e}')
        except json.JSONDecodeError as e:
            return RPCError(cls.PARSE_ERROR, f'cannot decode JSON: {e}')

    #
    # External API
    #
    @classmethod
    def process_message(cls, message):
        '''Returns an RPCRequest, RPCResponse or RPCBatch object.'''
        item = cls._message_to_item(message)
        if isinstance(item, dict):
            if 'method' in item:
                return cls._process_request(item)
            else:
                return cls._process_response(item)
        elif isinstance(item, list) and cls.allow_batches:
            return cls._process_batch(item)

        error = item
        if not isinstance(error, RPCError):
            error = cls.invalid_request('request object must be a dictionary')
        return RPCRequest(error, None, None)

    # Message formation
    @classmethod
    def item_bytes(cls, item):
        return getattr(cls, item.bytes_method)(item)

    @classmethod
    def request_bytes(cls, request):
        return cls.encode_payload(cls.request_payload(request))

    @classmethod
    def response_bytes(cls, response):
        return cls.encode_payload(cls.response_payload(response))

    @classmethod
    def error_bytes(cls, error):
        return cls.encode_payload(cls.error_payload(error))

    @classmethod
    def batch_bytes(cls, batch):
        if not cls.allow_batches:
            raise RuntimeError('protocol does not permit batches')
        rb = cls.request_bytes
        return cls.batch_bytes_from_parts(rb(item) for item in batch)

    @classmethod
    def batch_bytes_from_parts(cls, parts):
        '''Comma-separate the parts and wrap the lot in square brackets,
        except return no bytes if there are no parts.'''
        middle = b', '.join(parts)
        if middle:
            return b''.join([b'[', middle, b']'])
        return b''

    @classmethod
    def internal_error(cls, request_id):
        return RPCError(cls.INTERNAL_ERROR,
                        'internal error processing request', request_id)

    @classmethod
    def timeout_error(cls, request_id):
        return RPCError(cls.TIMEOUT_ERROR, 'request timed out', request_id)

    @classmethod
    def args_error(cls, message):
        return RPCError(cls.INVALID_ARGS, message, None)

    @classmethod
    def invalid_request(cls, message):
        return RPCError(cls.INVALID_REQUEST, message, None)

    @classmethod
    def method_not_found(cls, message):
        return RPCError(cls.METHOD_NOT_FOUND, message, None)

    @classmethod
    def encode_payload(cls, payload):
        '''Encode a Python object as binary bytes.'''
        try:
            return json.dumps(payload).encode()
        except TypeError:
            cls.logger.exception(f'payload JSON encoding failure: {payload}')
            if isinstance(payload, dict):
                request_id = payload.get('id')
            else:
                request_id = None
            return cls.error_bytes(cls.internal_error(request_id))

    @classmethod
    def detect_protocol(cls, message):
        '''Attempts to detect the protocol from the message.  If unsure
        returns None.'''
        main_item = cls._message_to_item(message)

        def protocol_for_item(item):
            if not isinstance(item, dict):
                return None
            # Obey an explicit "jsonrpc"
            version = item.get('jsonrpc')
            if version == '2.0':
                return JSONRPCv2
            if version == '1.0':
                return JSONRPCv1

            # Now to decide between JSONRPCLoose and JSONRPCv1 if possible
            if 'id' not in item:
                return JSONRPCLoose
            if 'result' in item:
                return JSONRPCv1 if 'error' in item else JSONRPCLoose
            if 'error' in item:
                return JSONRPCLoose
            if 'method' in item and 'params' not in item:
                return JSONRPCLoose
            return None

        if isinstance(main_item, list):
            parts = set(protocol_for_item(item) for item in main_item)
            # If all same protocol, return it
            if len(parts) == 1:
                return parts.pop()
            # If strict protocol detected, return it, preferring JSONRPCv2.
            # This means a batch of JSONRPCv1 will fail
            for protocol in (JSONRPCv2, JSONRPCv1):
                if protocol in parts:
                    return protocol
            if parts:
                return JSONRPCLoose
            return None

        return protocol_for_item(main_item)


class JSONRPCv1(JSONRPC):
    '''JSON RPC version 1.0.'''

    allow_batches = False

    @classmethod
    def _message_id(cls, message, require_id):
        # JSONv1 requires an ID always, but without constraint on its type
        # No need to test for a dictionary here as we don't handle batches.
        if 'id' not in message:
            raise cls.invalid_request('request has no "id"')
        return message['id']

    @classmethod
    def _request_args(cls, request):
        args = request.get('params')
        if not isinstance(args, list):
            if args is None:
                raise cls.args_error('no request arguments given')
            raise cls.args_error(f'invalid request arguments: {args}')
        return args

    @classmethod
    def _best_effort_error(cls, error):
        # Do our best to interpret the error
        code = cls.ERROR_CODE_UNAVAILABLE
        message = 'no error message provided'
        if isinstance(error, str):
            message = error
        elif isinstance(error, int):
            code = error
        elif isinstance(error, dict):
            if isinstance(error.get('message'), str):
                message = error['message']
            if isinstance(error.get('code'), int):
                code = error['code']

        return RPCError(code, message)

    @classmethod
    def response_result(cls, item):
        if 'result' not in item or 'error' not in item:
            raise cls.invalid_request(
                'response must contain both "result" and "error"')

        result = item['result']
        error = item['error']
        if error is None:
            return result   # It seems None can be a valid result
        if result is not None:
            raise cls.invalid_request(
                'response contains both "result" and "error"')

        return cls._best_effort_error(error)

    @classmethod
    def request_payload(cls, request):
        '''JSON v1 request (or notification) payload.'''
        assert isinstance(request, RPCRequest)
        if isinstance(request.args, dict):
            raise TypeError('JSONRPCv1 does not support named arguments')
        return {
            'method': request.method,
            'params': request.args,
            'id': request.request_id
        }

    @classmethod
    def response_payload(cls, response):
        '''JSON v1 response payload.'''
        assert isinstance(response, RPCResponse)
        result = response.result
        if isinstance(result, RPCError):
            return cls.error_payload(result)
        return {
            'result': result,
            'error': None,
            'id': response.request_id
        }

    @classmethod
    def error_payload(cls, error):
        assert isinstance(error, RPCError)
        return {
            'result': None,
            'error': {'code': error.code, 'message': error.message},
            'id': error.request_id
        }


class JSONRPCv2(JSONRPC):
    '''JSON RPC version 2.0.'''

    @classmethod
    def _message_id(cls, message, require_id):
        if not isinstance(message, dict):
            raise cls.invalid_request('request object must be a dictionary')
        if 'id' in message:
            request_id = message['id']
            if not isinstance(request_id, (numbers.Number, str, type(None))):
                raise cls.invalid_request(f'invalid "id": {request_id}')
            return request_id
        else:
            if require_id:
                raise cls.invalid_request('request has no "id"')
            return None

    @classmethod
    def _validate_message(cls, message):
        if message.get('jsonrpc') != '2.0':
            raise cls.invalid_request('"jsonrpc" is not "2.0"')

    @classmethod
    def _request_args(cls, request):
        args = request.get('params', [])
        if not isinstance(args, (dict, list)):
            raise cls.args_error(f'invalid request arguments: {args}')
        return args

    @classmethod
    def response_result(cls, item):
        if 'result' in item:
            if 'error' in item:
                raise cls.invalid_request(
                    'response contains both "result" and "error"')
            return item['result']

        if 'error' not in item:
            raise cls.invalid_request(
                'response contains neither "result" nor "error"')

        # Return an RPCError object
        error = item['error']
        if isinstance(error, dict):
            code = error.get('code')
            message = error.get('message')
            if isinstance(code, int) and isinstance(message, str):
                return RPCError(code, message)

        raise cls.invalid_request(f'ill-formed response error object: {error}')

    @classmethod
    def request_payload(cls, request):
        '''JSON v2 request (or notification) payload.'''
        assert isinstance(request, RPCRequest)
        payload = {
            'jsonrpc': '2.0',
            'method': request.method,
        }
        # A notification?
        if request.request_id is not None:
            payload['id'] = request.request_id
        # Preserve empty dicts as missing params is read as an array
        if request.args != []:
            payload['params'] = request.args
        return payload

    @classmethod
    def response_payload(cls, response):
        '''JSON v2 response payload.'''
        assert isinstance(response, RPCResponse)
        result = response.result
        if isinstance(result, RPCError):
            return cls.error_payload(result)

        return {
            'jsonrpc': '2.0',
            'result': result,
            'id': response.request_id
        }

    @classmethod
    def error_payload(cls, error):
        assert isinstance(error, RPCError)
        return {
            'jsonrpc': '2.0',
            'error': {'code': error.code, 'message': error.message},
            'id': error.request_id
        }


class JSONRPCLoose(JSONRPC):
    '''A relaxed versin of JSON RPC.'''

    # Don't be so loose we accept any old message ID
    _message_id = JSONRPCv2._message_id
    _validate_message = JSONRPC._validate_message
    _request_args = JSONRPCv2._request_args

    @classmethod
    def response_result(cls, item):
        # Return result, unless it is None and there is an error
        if item.get('error') is not None:
            if item.get('result') is not None:
                raise cls.invalid_request(
                    'response contains both "result" and "error"')
            return JSONRPCv1._best_effort_error(item['error'])

        if 'result' not in item:
            raise cls.invalid_request(
                'response contains neither "result" nor "error"')

        # Can be None
        return item['result']

    @classmethod
    def request_payload(cls, request):
        result = JSONRPCv2.request_payload(request)
        del result['jsonrpc']
        return result

    @classmethod
    def response_payload(cls, response):
        result = JSONRPCv2.response_payload(response)
        del result['jsonrpc']
        return result

    @classmethod
    def error_payload(cls, error):
        result = JSONRPCv2.error_payload(error)
        del result['jsonrpc']
        return result
