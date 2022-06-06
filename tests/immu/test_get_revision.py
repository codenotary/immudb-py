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

from grpc import RpcError
import uuid
import pytest
from tests.immuTestClient import ImmuTestClient

class TestGetRevision:

    def test_relative_get_revision(self, wrappedClient: ImmuTestClient):
        if(wrappedClient.serverHigherOrEqualsToVersion("1.3.0")):
            key = str(uuid.uuid4()).encode("utf-8")
            wrappedClient.set(key, b'0')
            wrappedClient.set(key, b'1')
            wrappedClient.set(key, b'2')

            now = wrappedClient.get(key, 0)
            assert now.value == b'2'
            assert now.revision == 3
            now = wrappedClient.get(key, -1)
            assert now.value == b'1'
            assert now.revision == 2
            now = wrappedClient.get(key, -2)
            assert now.value == b'0'
            assert now.revision == 1
            with pytest.raises(RpcError, match="invalid key revision number") :
                now = wrappedClient.get(key, -3)

    def test_non_relative_get_revision(self, wrappedClient: ImmuTestClient):
        if(wrappedClient.serverHigherOrEqualsToVersion("1.3.0")):
            key = str(uuid.uuid4()).encode("utf-8")
            wrappedClient.set(key, b'033')
            wrappedClient.set(key, b'133')
            wrappedClient.set(key, b'233')
            
            now = wrappedClient.get(key, 1)
            assert now.value == b'033'
            assert now.revision == 1
            now = wrappedClient.get(key, 2)
            assert now.value == b'133'
            assert now.revision == 2
            now = wrappedClient.get(key, 3)
            assert now.value == b'233'
            assert now.revision == 3

            with pytest.raises(RpcError, match="invalid key revision number") :
                now = wrappedClient.get(key, 4)
        else:
            pytest.skip("Server does not support yet revisions")


    def test_relative_verified_get_revision(self, wrappedClient: ImmuTestClient):
        if(wrappedClient.serverHigherOrEqualsToVersion("1.3.0")):
            key = str(uuid.uuid4()).encode("utf-8")
            wrappedClient.set(key, b'0')
            wrappedClient.set(key, b'1')
            wrappedClient.set(key, b'2')

            now = wrappedClient.verifiedGet(key, 0)
            assert now.value == b'2'
            assert now.revision == 3
            assert now.verified == True
            now = wrappedClient.verifiedGet(key, -1)
            assert now.value == b'1'
            assert now.revision == 2
            assert now.verified == True
            now = wrappedClient.verifiedGet(key, -2)
            assert now.value == b'0'
            assert now.revision == 1
            assert now.verified == True
            with pytest.raises(RpcError, match="invalid key revision number") :
                now = wrappedClient.verifiedGet(key, -3)

    def test_non_relative_verified_get_revision(self, wrappedClient: ImmuTestClient):
        if(wrappedClient.serverHigherOrEqualsToVersion("1.3.0")):
            key = str(uuid.uuid4()).encode("utf-8")
            wrappedClient.set(key, b'033')
            wrappedClient.set(key, b'133')
            wrappedClient.set(key, b'233')
            
            now = wrappedClient.verifiedGet(key, 1)
            assert now.value == b'033'
            assert now.revision == 1
            assert now.verified == True
            now = wrappedClient.verifiedGet(key, 2)
            assert now.value == b'133'
            assert now.revision == 2
            assert now.verified == True
            now = wrappedClient.verifiedGet(key, 3)
            assert now.value == b'233'
            assert now.revision == 3
            assert now.verified == True

            with pytest.raises(RpcError, match="invalid key revision number") :
                now = wrappedClient.verifiedGet(key, 4)
        else:
            pytest.skip("Server does not support yet revisions")