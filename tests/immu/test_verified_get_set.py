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


class TestVerifiedGetSet:

    def test_verified(self, client: ImmudbClient):

        key1 = "verified_key_{:04d}".format(randint(0, 10000))
        value1 = "verified_value_{:04d}".format(randint(0, 10000))

        resp = client.verifiedSet(
            key1.encode('utf8'), value1.encode('utf8'))

        readback1 = client.verifiedGet(key1.encode('utf8'))
        assert value1 == readback1.value.decode('utf8')

        tx1id = readback1.id

        key2 = "verified_key_{:04d}".format(randint(0, 10000))
        value2 = "verified_value_{:04d}".format(randint(0, 10000))

        resp = client.verifiedSet(
            key2.encode('utf8'), value2.encode('utf8'))

        readback2 = client.verifiedGet(key2.encode('utf8'))
        assert value2 == readback2.value.decode('utf8')

        tx2id = readback2.id

        readback3 = client.verifiedGetAt(key1.encode('utf8'), atTx=tx1id)
        assert value1 == readback3.value.decode('utf8')

        with pytest.raises(grpc._channel._InactiveRpcError):
            readback4 = client.verifiedGetAt(key2.encode('utf8'), atTx=tx1id)

        readback5 = client.verifiedGetSince(key1.encode('utf8'), sinceTx=tx1id)
        assert value1 == readback5.value.decode('utf8')

        with pytest.raises(grpc._channel._InactiveRpcError):
            readback6 = client.verifiedGetSince(
                key1.encode('utf8'), sinceTx=tx2id)
