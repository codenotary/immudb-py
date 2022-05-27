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
import pytest
import grpc._channel
import threading
import time
from tests.immuTestClient import ImmuTestClient

def setAfter(client, toWait, key, value):
    time.sleep(toWait)
    client.set(key, value)

class TestVerifiedGetSet:

    def test_verified(self, wrappedClient: ImmuTestClient):

        key1 = "verified_key_{:04d}".format(randint(0, 10000))
        value1 = "verified_value_{:04d}".format(randint(0, 10000))

        resp = wrappedClient.client.verifiedSet(
            key1.encode('utf8'), value1.encode('utf8'))

        readback1 = wrappedClient.client.verifiedGet(key1.encode('utf8'))
        assert value1 == readback1.value.decode('utf8')

        tx1id = readback1.id

        key2 = "verified_key_{:04d}".format(randint(0, 10000))
        value2 = "verified_value_{:04d}".format(randint(0, 10000))

        resp = wrappedClient.client.verifiedSet(
            key2.encode('utf8'), value2.encode('utf8'))

        readback2 = wrappedClient.client.verifiedGet(key2.encode('utf8'))
        assert value2 == readback2.value.decode('utf8')

        tx2id = readback2.id

        readback3 = wrappedClient.client.verifiedGetAt(key1.encode('utf8'), atTx=tx1id)
        assert value1 == readback3.value.decode('utf8')

        with pytest.raises(grpc._channel._InactiveRpcError):
            readback4 = wrappedClient.client.verifiedGetAt(key2.encode('utf8'), atTx=tx1id)

        readback5 = wrappedClient.client.verifiedGetSince(key1.encode('utf8'), sinceTx=tx1id)
        assert value1 == readback5.value.decode('utf8')

        key2 = "verified_key_{:04d}".format(randint(0, 10000)).encode("utf-8")
        value2 = "verified_value_{:04d}".format(randint(0, 10000)).encode("utf-8")
        
        if(wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            # Startin from 1.2.0 it will return an error if txId not exists
            with pytest.raises(grpc._channel._InactiveRpcError):
                readback6 = wrappedClient.client.verifiedGetSince(key1.encode('utf8'), sinceTx = tx2id + 1)
            readback6 = wrappedClient.client.verifiedGetSince(key1.encode('utf8'), sinceTx = tx2id)
            assert readback6.value.decode("utf-8") == value1
        else:
            readback6 = wrappedClient.client.verifiedGetSince(key1.encode('utf8'), sinceTx = tx2id)
            assert readback6.value.decode("utf-8") == value1
            # Get a non existing transaction, verified Get Since will block until other thread will fill the data and make transaction existing
            threading.Thread(target=setAfter, args=(wrappedClient.client, 1.5, key2, value2)).start()
            readback6 = wrappedClient.client.verifiedGetSince(key1.encode('utf8'), sinceTx = tx2id + 1)
            assert readback6.value.decode("utf-8") == value1
