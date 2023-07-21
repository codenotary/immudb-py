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

from immudb.client import ImmudbClient
import pytest
from grpc import RpcError, StatusCode
import uuid

def generateBigDict():
    toBuild = dict()
    keyList = []
    for _ in range(0, 128):
        bigStr = str(uuid.uuid4()) * 512
        key = str(uuid.uuid4()).encode("utf-8")
        keyList.append(key)
        toBuild[key] = bigStr.encode("utf-8")
    return toBuild, keyList


class TestTimeout:
    def test_simple_timeout(self, argsToBuildClient):
        url, login, password = argsToBuildClient
        client = ImmudbClient(url, timeout=1)
        try:
            client.login(login, password)
        except:
            raise pytest.skip('Cannot reach immudb server')

        with pytest.raises(RpcError) as excinfo: 
            client = ImmudbClient(url, timeout=0.001)
            client.login(login, password)
        assert excinfo.value.code() == StatusCode.DEADLINE_EXCEEDED


        with pytest.raises(RpcError) as excinfo: 
            client = ImmudbClient(url, timeout=0.05)
            client.login(login, password)
            keyVal, keys = generateBigDict()
            client.setAll(keyVal)
            scanned = client.scan(b"", b"", False, 400)
        assert excinfo.value.code() == StatusCode.DEADLINE_EXCEEDED
        
