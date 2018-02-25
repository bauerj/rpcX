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


import asyncio
from functools import partial
import logging
import time


class Session(asyncio.Protocol):
    '''An RPC session for use with asyncio.

    Derived classes may want to override methods in the App Layer below.
    '''
    _next_session_id = 0

    # Error Codes
    OVERSIZED_MESSAGE = 1

    @classmethod
    def next_session_id(cls):
        '''Return the next unique session ID.'''
        session_id = cls._next_session_id
        cls._next_session_id += 1
        return session_id

    def __init__(self, rpc_processor, framer, logger=None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.rpc = rpc_processor
        self.framer = framer
        self.error_count = 0
        self.session_id = None
        # Write buffer handling
        self.pause = False
        self.paused_bytes = []
        # Transport must implement write() and is_closing()
        self.transport = None
        # Incoming message statistics.  last_recv is only updated when
        # a full RPC request has been received
        self.recv_size = 0
        self.recv_count = 0
        self.last_recv = time.time()
        # Incoming message statistics.  last_recv is only updated when
        # a full RPC request has been received
        self.send_size = 0
        self.send_count = 0

    def connection_made(self, transport):
        '''Call when a connection is established.'''
        self.transport = transport
        self.session_id = self.next_session_id()
        transport.set_write_buffer_limits(high=500000)

    def connection_lost(self, exc):
        '''Close the RPC proessor.'''
        self.rpc.close()

    def pause_writing(self):
        '''Transport calls when the send buffer is full.'''
        self.logger.info('pausing writing whilst socket drains')
        self.pause = True

    def resume_writing(self):
        '''Transport calls when the send buffer has room.'''
        self.logger.info('resuming writing')
        self.pause = False
        # Send any paused items (in reverse order...)
        while self.paused_items and not self.pause:
            self.send_item(self.paused_items.pop())

    def data_recieved(self, data):
        '''Transport calls with incoming data.'''
        self.recv_size += len(data)
        self.using_bandwidth(len(data))
        count = self.rpc.data_received(data)
        if count:
            self.recv_count += count
            self.last_recv = time.time()

    def _send_binary(self, binary):
        if self.tranport.is_closing():
            return
        frame = self.framer.frame(binary)
        self.transport.write_lines(frame)
        size = sum(len(part) for part in frame)
        self.using_bandwidth(size)
        self.send_size += size
        self.send_count += 1

    def _send_item(self, item):
        '''Send an RPC object.'''
        if self.pause:
            self.paused_items.append(item)
        else:
            self._send_binary(self.rpc.item_bytes(item))

    # External API
    def send_request(self, handler, method, args=[], timeout=30):
        self.send_item(RPCRequest.create_next(method, args, handler, timeout))

    def send_notification(self, method, args=[]):
        self.send_item(RPCRequest(method, args, None))

    def peer_info(self):
        '''Returns information about the peer.'''
        try:
            # get_extra_info can throw even if self.transport is not None
            # I believe this was a bug in older versions of Python
            return self.transport.get_extra_info('peername')
        except Exception:
            return None

    def peer_addr(self, anon=True):
        '''Return the peer address and port.'''
        peer_info = self.peer_info()
        if not peer_info:
            return 'unknown'
        if anon:
            return 'xx.xx.xx.xx:xx'
        if ':' in peer_info[0]:
            return '[{}]:{}'.format(peer_info[0], peer_info[1])
        else:
            return '{}:{}'.format(peer_info[0], peer_info[1])

    def close_connection(self):
        '''Close the connection.'''
        if self.transport:
            self.transport.close()

    def abort(self):
        '''Cut the connection abruptly.'''
        self.transport.abort()

    def using_bandwidth(self, amount):
        '''Called as bandwidth is consumed.

        Override to implement bandwidth management.
        '''
        pass
