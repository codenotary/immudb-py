# Copyright 2021 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from immudb.client import ImmudbClient
from random import randint
import grpc._channel
import warnings


class TestBasicGetSet:
    def test_no_server(self):
        try:
            a = ImmudbClient("localhost:9999")
            a.login("immudb", "immudb")
        except grpc.RpcError:
            pass

    def test_basic(self, client):
        key = "test_key_{:04d}".format(randint(0, 10000))
        value = "test_value_{:04d}".format(randint(0, 10000))

        resp = client.set(key.encode('utf8'), value.encode('utf8'))
        readback = client.get(key.encode('utf8'))
        assert value == readback.value.decode('utf8')
        val = client.getValue(key.encode('utf8'))
        assert val == value.encode('utf-8')
        assert None == client.getValue(b'non_existing_key')
        assert client.healthCheck()
        client.logout()
        client.shutdown()

    def test_root(self, client):
        r1 = client.currentState()
        key = "test_key_{:04d}".format(randint(0, 10000))
        value = "test_value_{:04d}".format(randint(0, 10000))
        client.verifiedSet(key.encode('utf8'), value.encode('utf8'))
        r2 = client.currentState()

        assert r2.txId > r1.txId
        client.logout()
        client.shutdown()

    def test_property(self, client):
        assert isinstance(client.stub, object)

    def test_safeSet(self, client):
        r1 = client.currentState()
        key = "test_key_{:04d}".format(randint(0, 10000))
        value = "test_value_{:04d}".format(randint(0, 10000))
        # explicitly test deprecated safeSet
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        client.safeSet(key.encode('utf8'), value.encode('utf8'))
        client.safeGet(key.encode('utf8'))

    def test_get_tx(self, client):
        key0 = "test_key_{:04d}".format(randint(0, 10000)).encode('ascii')
        value0 = "test_value_{:04d}".format(randint(0, 10000)).encode('ascii')
        id0 = client.verifiedSet(key0, value0).id
        assert key0 in client.txById(id0)
        assert key0 in client.verifiedTxById(id0)
        assert client.txById(id0 + 100) is None
        assert client.verifiedTxById(id0 + 100) is None
        for i in range(0, 3):
            key = "test_key_{:04d}".format(randint(0, 10000)).encode('ascii')
            value = "test_value_{:04d}".format(
                randint(0, 10000)).encode('ascii')
            client.verifiedSet(key, value)
        assert key0 in client.verifiedTxById(id0)
