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


class TestGetSet:

    def test_get_set_uniq(self, client):
        kvs = []
        for t in range(0, 100):
            value = "test_value_{:04d}".format(randint(0, 10000))
            while True:
                key = "test_key_{:04d}".format(randint(0, 10000))
                if key not in [t[0] for t in kvs]:
                    break
            kvs.append((key, value))
            resp = client.verifiedSet(key.encode('utf8'), value.encode('utf8'))
            assert resp.verified
        for (key, value) in kvs:
            readback = client.verifiedGet(key.encode('utf8'))
            print(key, value, readback.value)
            assert readback.verified
            assert value.encode('utf8') == readback.value

    def test_get_set_over(self, client):
        kvs = {}
        for t in range(0, 300):
            key = "test_key_{:04d}".format(randint(0, 100))
            value = "test_value_{:04d}".format(randint(0, 100))
            kvs[key] = value
            resp = client.safeSet(key.encode('utf8'), value.encode('utf8'))
            assert resp.verified
        for (key, value) in kvs.items():
            readback = client.safeGet(key.encode('utf8'))
            print(key, value, readback.value)
            assert readback.verified
            assert value.encode('utf8') == readback.value

    def test_get_set_batch(self, client):
        xset = {
            b'gorilla': b'banana',
            b'zebra':   b'grass',
            b'lion':    b'zebra'
        }
        assert type(client.setAll(xset)) != int
        # test getAll
        resp = client.getAll(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i] == resp[i]
        for i in xset.keys():
            assert i in resp
            assert xset[i] == resp[i]
        # test getAllItems
        resp = client.getAll(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i] == resp[i]

    def test_get_set_over(self, client):
        r = client.get(b"not:existing:key")
        assert r == None

    def test_set_all_get_one(self, client):
        kvs = {}
        for t in range(0, 100):
            value = "test_sago_value_{:04d}".format(randint(0, 10000))
            while True:
                key = "test_sago_key_{:04d}".format(randint(0, 10000))
                if key not in kvs:
                    break
            kvs[key.encode('ascii')] = value.encode('ascii')
        resp = client.setAll(kvs)
        for k in kvs.keys():
            ret = client.verifiedGet(k)
            assert ret.verified
