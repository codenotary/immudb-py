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


class TestTimeout:
    def test_simple_timeout(self, argsToBuildClient):
        url, login, password = argsToBuildClient
        client = ImmudbClient(url, timeout=1)
        try:
            client.login(login, password)
        except:
            raise pytest.skip('Cannot reach immudb server')

        # The timeout also applies to calls after login.
        client.set(b"timeout_test_key", b"value")
        assert client.get(b"timeout_test_key").value == b"value"

        # A 1 microsecond deadline expires before any server can answer,
        # so the result does not depend on how fast the runner is.
        with pytest.raises(RpcError) as excinfo:
            client = ImmudbClient(url, timeout=0.000001)
            client.login(login, password)
        assert excinfo.value.code() == StatusCode.DEADLINE_EXCEEDED
