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
import random,string
import grpc._channel

def get_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for i in range(length))

        
def test_reference():
    try:
        a = ImmudbClient("localhost:3322")
        a.login("immudb","immudb")
    except grpc._channel._InactiveRpcError as e:
        pytest.skip("Cannot reach immudb server")
    k="reftest.key."+get_random_string(16)
    v=get_random_string(32)
    r1="reftest.reference."+get_random_string(16)
    r2="reftest.reference."+get_random_string(16)
    setresp=a.verifiedSet(k.encode('ascii'),v.encode('ascii'))
    a.setReference(k.encode('ascii'),r1.encode('ascii'))
    a.verifiedSetReference(k.encode('ascii'),r2.encode('ascii'))
    referred1=a.verifiedGet(r1.encode('ascii'))
    referred2=a.verifiedGet(r1.encode('ascii'))
    original=a.verifiedGet(k.encode('ascii'))
    assert original.key==referred1.key
    assert original.value==referred1.value
    assert original.key==referred2.key
    assert original.value==referred2.value
