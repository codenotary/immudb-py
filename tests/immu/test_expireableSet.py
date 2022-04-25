# Copyright 2022 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pytest
from random import randint

from tests.immuTestClient import ImmuTestClient
import datetime
import time
import grpc._channel


class TestExpireableSet:

    def test_expireableSet(self, wrappedClient: ImmuTestClient):

        if(wrappedClient.serverHigherOrEqualsToVersion("1.2.1")):
            key = "expire_key_{:04d}".format(randint(0, 10000))
            value = "expire_value_{:04d}".format(randint(0, 10000))

            expiresAt = datetime.datetime.now()+datetime.timedelta(seconds=5)

            resp = wrappedClient.client.expireableSet(
                key.encode('utf8'), value.encode('utf8'), expiresAt)
            readback = wrappedClient.client.verifiedGet(key.encode('utf8'))
            assert value == readback.value.decode('utf8')

            time.sleep(6)

            with pytest.raises(grpc._channel._InactiveRpcError):
                verifiedExpiredReadback = wrappedClient.client.verifiedGet(
                    key=key.encode('utf8'))
            with pytest.raises(grpc._channel._InactiveRpcError):
                expiredReadback = wrappedClient.client.get(
                    key=key.encode('utf8'))
        else:
            pytest.skip()
