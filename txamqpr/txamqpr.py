# Copyright 2015 Alexey Vishnevsky aliowka@gmail.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from txamqp import spec
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from twisted.internet import protocol, reactor, task
from twisted.internet.defer import inlineCallbacks, Deferred, returnValue, DeferredList


class AmqpProtocol(AMQClient):
    """
    The protocol is created and destroyed each time a connection is created and lost.
    """

    def __init__(self, delegate, vhost, spec, factory):
        AMQClient.__init__(self, delegate, vhost, spec)
        self.factory = factory

    def connectionMade(self):
        """
        Called when a connection has been made.
        """
        AMQClient.connectionMade(self)

        self.connected = False
        d = self.start({"LOGIN": self.factory.user, "PASSWORD": self.factory.password})
        d.addCallback(self._authenticated)
        d.addErrback(self._authentication_failed)

    def _authenticated(self, ignore):
        """
        Called when the connection has been authenticated.
        """

        d = self.channel(1)
        d.addCallback(self._got_channel)
        d.addErrback(self._got_channel_failed)

    def _got_channel(self, chan):
        self.chan = chan

        d = self.chan.channel_open()
        d.addCallback(self._channel_open)
        d.addErrback(self._channel_open_failed)

    def _channel_open(self, arg):
        """Called when the channel is open."""

        reactor.callLater(0, self.setup_environment)

    @inlineCallbacks
    def setup_environment(self):
        try:
            yield self.chan.exchange_declare(**self.factory.exchange_conf)
            yield self.chan.queue_declare(**self.factory.queue_declare_conf)
            yield self.chan.queue_bind(**self.factory.queue_binding_conf)
            yield self.chan.basic_qos(prefetch_count=self.factory.prefetch)

            self.connected = True
            reactor.callLater(0, self.factory.deferred.callback, None)
        except Exception as e:
            self.factory.deferred.errback(e)

    def _channel_open_failed(self, error):
        pass

    def _got_channel_failed(self, error):
        pass

    def _authentication_failed(self, error):
        pass

    def basic_publish(self, msg, properties):
        return self.chan.basic_publish(exchange=self.factory.exchange_conf["exchange"],
                                       routing_key=self.factory.queue_binding_conf["routing_key"],
                                       content=Content(msg, properties=properties))

    def basic_get(self, queue, no_ack):
        return self.chan.basic_get(queue=queue, no_ack=no_ack)

    def basic_ack(self, msg):
        return self.chan.basic_ack(delivery_tag=msg.delivery_tag)


class txAMQPReconnectingFactory(protocol.ReconnectingClientFactory):
    protocol = AmqpProtocol

    def __init__(self, spec_file=None,
                 host='localhost', port=5672, user="guest",
                 password="guest", prefetch=1, vhost="/",
                 exchange_conf=None, queue_declare_conf=None, queue_binding_conf=None):
        self.queue_binding_conf = queue_binding_conf
        self.queue_declare_conf = queue_declare_conf
        self.exchange_conf = exchange_conf
        spec_file = spec_file or os.path.join(os.path.dirname(__file__), 'amqp0-8.stripped.rabbitmq.xml')
        self.spec = spec.load(spec_file)
        self.user = user
        self.password = password
        self.vhost = vhost
        self.host = host
        self.delegate = TwistedDelegate()
        self.deferred = Deferred()
        self.prefetch = prefetch

        self.p = None  # The protocol instance.
        self.client = None  # Alias for protocol instance
        self.port = reactor.connectTCP(self.host, port, self)

    def buildProtocol(self, addr):
        self.client = self.protocol(factory=self,
                                    delegate=self.delegate,
                                    vhost=self.vhost,
                                    spec=self.spec)
        self.p = self.client
        self.resetDelay()
        return self.p

    def clientConnectionFailed(self, connector, reason):
        self.p = None
        self.client = None
        self.deferred = Deferred()
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        self.p = None
        self.client = None
        self.deferred = Deferred()
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def basic_get(self, queue, no_ack):
        # if self.p and self.p.connected:
        #     return self.p.basic_get(queue, no_ack)
        result = self.deferred.addCallback(lambda *args: self.p.basic_get(queue, no_ack))
        return result

    def basic_publish(self, msg, properties):

        if self.p and self.p.connected:
            return self.p.basic_publish(msg, properties)

        self.deferred.addCallback(lambda *args: self.p.basic_publish(msg, properties))
        return self.deferred

    def basic_ack(self, msg):
        if self.p and self.p.connected:
            return self.p.basic_ack(msg)

        self.deferred.addCallback(lambda *args: self.p.basic_ack(msg))
        return self.deferred

    def _disconnect(self):
        if self.p and self.p.connected:
            return self.p.transport.loseConnection()

        self.deferred.addCallback(lambda *args: self.p.transport.loseConnection)
        return self.deferred

