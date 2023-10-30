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

import string
import random
import time
from tests.immuTestClient import ImmuTestClient
import pytest
import grpc


def get_random_string(length):
    return ''.join(random.choice(string.printable) for i in range(length))


class TestGetSet:
    def test_get_set_massive(self, client):
        xset = {}
        for i in range(0, 1000):
            xset["massif:{:04X}".format(i).encode(
                'utf8')] = get_random_string(32).encode('utf8')
        assert type(client.setAll(xset)) != int
        time.sleep(2)
        # test getAllValues
        resp = client.getAllValues(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i] == resp[i].value
        for i in xset.keys():
            assert i in resp
            assert xset[i] == resp[i].value
        # test getAll
        resp = client.getAll(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i] == resp[i]

    def test_compact(self, wrappedClient: ImmuTestClient):
        # Snapshot compaction fixed from 1.3.1
        if(wrappedClient.serverHigherOrEqualsToVersion("1.3.0")):
            try:
                wrappedClient.client.compactIndex()
            except grpc._channel._InactiveRpcError as e:
                if e.details() == "tbtree: compaction threshold not yet reached":
                    print(e)
                else:
                    raise e
        else:
            pytest.skip("Server version too low")
