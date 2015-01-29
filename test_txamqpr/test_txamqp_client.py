# Copyright 2015 Alexey Vishnevsky aliowka@gmail.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import random
from twisted.internet.task import deferLater, LoopingCall, Clock
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredList, maybeDeferred
from twisted.trial import unittest
from txamqpr import txAMQPReconnectingFactory


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.published_counter = 0
        self.fetched_counter = 0
        self.random_name = "test-txamqpr-client-%s" % random.randint(0, sys.maxint)

        rabbitmq_conf = {
            "exchange_conf": {
                "exchange": self.random_name,
                "type": "fanout",
                "durable": False,
                "auto_delete": True},
            "queue_declare_conf": {
                "queue": self.random_name,
                "durable": False,
                "exclusive": False,
                "arguments": {"x-expires": 30000}},
            "queue_binding_conf": {
                "exchange": self.random_name,
                "queue": self.random_name,
                "routing_key": self.random_name}}

        self.tx = txAMQPReconnectingFactory(**rabbitmq_conf)

    def get_message(self, no_ack=True):

        def on_message(msg):
            if msg.method.name != "get-empty":
                self.assertEqual(msg.content.body, "Hello")
                return msg

            if hasattr(self, "disconnector"):
                self.disconnector.stop()
            self.message_getter.stop()
            if self.show_stoper:
                reactor.callLater(5, self.show_stoper.callback, None)
                self.show_stoper = None

        def on_error(*args):
            pass

        if no_ack:
            ack_callback = lambda msg: msg
        else:
            ack_callback = lambda msg: self.tx.basic_ack(msg)

        d = self.tx.basic_get(self.random_name, no_ack)
        d.addCallback(on_message).addCallback(ack_callback)
        d.addErrback(on_error)

    def publish_message(self):
        self.tx.basic_publish("Test message", None)
        self.published_counter += 1
        if self.published_counter > 999:
            self.publisher.stop()

    @inlineCallbacks
    def test_pub_and_sub(self):
        yield self.tx.deferred
        self.show_stoper = Deferred()

        self.publisher = LoopingCall(self.publish_message)
        self.message_getter = LoopingCall(self.get_message)

        self.publisher.start(0.01)
        self.message_getter.start(0.01)
        yield self.show_stoper

    @inlineCallbacks
    def test_pub_and_sub_while_disconnect(self):
        yield self.tx.deferred
        self.show_stoper = Deferred()

        self.publisher = LoopingCall(self.publish_message)
        self.message_getter = LoopingCall(self.get_message)
        self.disconnector = LoopingCall(self.tx._disconnect)
        self.disconnector.start(1)
        self.publisher.start(0.01)
        self.message_getter.start(0.01)
        yield self.show_stoper

    @inlineCallbacks
    def test_pub_and_sub_and_ack(self):
        yield self.tx.deferred
        self.show_stoper = Deferred()

        self.publisher = LoopingCall(self.publish_message)
        self.message_getter = LoopingCall(self.get_message, no_ack=False)

        self.publisher.start(0.01)
        self.message_getter.start(0.01)
        yield self.show_stoper

    @inlineCallbacks
    def test_pub_and_sub_and_ack_with_disconnect(self):
        yield self.tx.deferred
        self.show_stoper = Deferred()

        self.publisher = LoopingCall(self.publish_message)
        self.message_getter = LoopingCall(self.get_message, no_ack=False)
        self.disconnector = LoopingCall(self.tx._disconnect)
        self.disconnector.start(1)
        self.publisher.start(0.01)
        self.message_getter.start(0.01)
        yield self.show_stoper


    def tearDown(self):
        self.tx.stopTrying()
        self.tx.p.transport.loseConnection()
