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

import pytest
from immudb.client import ImmudbClient
import string,random
import grpc._channel
import time


def get_random_string(length):
    return ''.join(random.choice(string.printable) for i in range(length))

class TestGetSet:
    def test_get_set_massive(self):
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        xset={}
        for i in range(0,1000):
            xset["massif:{:04X}".format(i).encode('utf8')]=get_random_string(32).encode('utf8')
        assert type(a.setAll(xset))!=int
        time.sleep(2)
        # test getAllValues
        resp=a.getAllValues(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i]==resp[i].value
        for i in xset.keys():
            assert i in resp
            assert xset[i]==resp[i].value
        # test getAll
        resp=a.getAll(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i]==resp[i]



