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
from immudb.datatypes import KeyValue, ZAddRequest, ReferenceRequest
from random import randint
import grpc._channel
import time


class TestExecAll:

    def test_execAll_KV(self, client):
        kvCompare = []
        kvsend = []
        for t in range(0, 100):
            value = "execAll_KV_value_{:04d}".format(randint(0, 10000))
            while True:
                key = "execAll_KV_key_{:04d}".format(randint(0, 10000))
                if key not in [t[0] for t in kvCompare]:
                    break
            kvCompare.append((key, value))
            kvsend.append(KeyValue(key=key.encode(
                'utf8'), value=value.encode('utf8')))
        resp = client.execAll(kvsend)
        assert resp.nentries == len(kvsend)
        for (key, value) in kvCompare:
            readback = client.verifiedGet(key.encode('utf8'))
            print(key, value, readback.value)
            assert readback.verified
            assert value.encode('utf8') == readback.value

    def test_execAll_ref(self, client):
        kvsend = []
        refCompare = []
        refsend = []
        for t in range(0, 100):
            key = "execAll_ref_key_{:04d}".format(t)
            value = "execAll_ref_value_{:04d}".format(t)
            kvsend.append(KeyValue(key=key.encode(
                'utf8'), value=value.encode('utf8')))
            refkey = "execAll_ref_refkey_{:04d}".format(t)
            refCompare.append((key, refkey, value))
            refsend.append(ReferenceRequest(key=refkey.encode(),
                                            referencedKey=key.encode()))
        resp = client.execAll(kvsend)
        assert resp.nentries == len(kvsend)
        resp = client.execAll(refsend)
        assert resp.nentries == len(refsend)

        for (key, refkey, value) in refCompare:
            readback = client.verifiedGet(refkey.encode('utf8'))
            assert readback.key == key.encode()
            assert readback.value == value.encode()

    def test_execAll_zadd(self):
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb", "immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        kvsend = []
        zaddsend = []
        zsetname = "execAll_zadd_set_{:04d}".format(
            randint(0, 10000)).encode('utf-8')
        for t in range(0, 10):
            key = "execAll_zadd_key_{:04d}".format(t)
            value = "execAll_zadd_value_{:04d}".format(t)
            score = float(t)

            kvsend.append(KeyValue(key=key.encode(
                'utf8'), value=value.encode('utf8')))

            zaddsend.append(ZAddRequest(key=key.encode(),
                                        set=zsetname, score=score))

        resp = a.execAll(kvsend)
        assert resp.nentries == len(kvsend)
        resp = a.execAll(zaddsend)
        assert resp.nentries == len(zaddsend)

        resp = a.zScan(zsetname, b'execAll_zadd_set_', 0, 0,
                       True, 100, False, 0.0, 100.0, 0)

        assert len(resp.entries) == 10

    def test_execAll_unknown(self, client):
        with pytest.raises(Exception) as e_info:
            bla = [{"foo": "bar"}]
            resp = client.execAll(bla)
