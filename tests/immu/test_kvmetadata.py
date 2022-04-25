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
from immudb.handler import verifiedSet
from immudb.embedded.store import KVMetadata
from immudb.exceptions import ErrCorruptedData


class TestKVMetadata:

    def test_kvmetadataproof(self, wrappedClient: ImmuTestClient):

        if(wrappedClient.serverHigherOrEqualsToVersion("1.2.2")):
            key = "metadata_key_{:04d}".format(randint(0, 10000))
            value = "metadata_value_{:04d}".format(randint(0, 10000))

            expiresAt = datetime.datetime.now()+datetime.timedelta(seconds=5)

            metadata = KVMetadata()
            metadata.ExpiresAt(expiresAt)
            metadata.AsNonIndexable(True)

            resp = verifiedSet.call(wrappedClient.client._ImmudbClient__stub,
                                    wrappedClient.client._ImmudbClient__rs, key.encode("utf8"), value.encode("utf8"), metadata=metadata)

            assert resp.verified == True

            metadata.AsDeleted(True)

            with pytest.raises(ErrCorruptedData):
                resp = verifiedSet.call(wrappedClient.client._ImmudbClient__stub,
                                        wrappedClient.client._ImmudbClient__rs, key.encode("utf8"), value.encode("utf8"), metadata=metadata)
        else:
            pytest.skip()
