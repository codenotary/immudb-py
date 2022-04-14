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


from random import randint

from immudb.datatypes import DeleteKeysRequest

from tests.immuTestClient import ImmuTestClient
import immudb.constants


class TestDelete:

    def test_delete(self, wrappedClient: ImmuTestClient):

        if(wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            key = "delete_key_{:04d}".format(randint(0, 10000))
            value = "delete_value_{:04d}".format(randint(0, 10000))

            resp = wrappedClient.client.set(
                key.encode('utf8'), value.encode('utf8'))
            readback = wrappedClient.client.get(key.encode('utf8'))
            assert value == readback.value.decode('utf8')

            req = DeleteKeysRequest([key.encode('utf8')])

            resp = wrappedClient.client.delete(req)
            txid = resp.id

            assert resp.nentries == 1

            readback = wrappedClient.client.get(key.encode('utf8'))
            assert readback == None  # its gone...

            hist = wrappedClient.client.history(key.encode('utf8'), 0, 99,
                                                immudb.constants.NEWEST_FIRST)

            assert len(hist) == 2  # ...but still there

            verifiedDeletedReadback = wrappedClient.client.verifiedGetAt(
                key=key.encode('utf8'), atTx=txid)
