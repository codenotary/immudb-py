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
from immudb.client import ImmudbClient, PersistentRootService
from random import randint
import grpc._channel
import warnings


def test_rs(rootfile):
    rs = PersistentRootService()
    s = None
    try:
        s = rs.get()
    except:
        pass  # it is expected to fail
    assert s == None


def test_basic(rootfile):
    try:
        a = ImmudbClient(rs=PersistentRootService(rootfile))
        a.login("immudb", "immudb")
    except grpc.RpcError as e:
        pytest.skip("Cannot reach immudb server")
    key = "test_key_{:04d}".format(randint(0, 10000))
    value = "test_value_{:04d}".format(randint(0, 10000))

    resp = a.set(key.encode('utf8'), value.encode('utf8'))
    readback = a.get(key.encode('utf8'))
    assert value == readback.value.decode('utf8')
    val = a.getValue(key.encode('utf8'))
    assert val == value.encode('utf-8')
    assert None == a.getValue(b'non_existing_key')
    assert a.healthCheck()
    a.logout()
    a.shutdown()


def test_root(client_rs):
    r1 = client_rs.currentState()
    key = "test_key_{:04d}".format(randint(0, 10000))
    value = "test_value_{:04d}".format(randint(0, 10000))
    client_rs.verifiedSet(key.encode('utf8'), value.encode('utf8'))
    r2 = client_rs.currentState()

    assert r2.txId > r1.txId
    client_rs.logout()
    client_rs.shutdown()


def test_property(client_rs):
    assert isinstance(client_rs.stub, object)


def test_safeSet(client_rs):
    r1 = client_rs.currentState()
    key = "test_key_{:04d}".format(randint(0, 10000))
    value = "test_value_{:04d}".format(randint(0, 10000))
    # explicitly test deprecated safeSet
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    client_rs.safeSet(key.encode('utf8'), value.encode('utf8'))
    client_rs.safeGet(key.encode('utf8'))


def test_get_tx(client_rs):
    key0 = "test_key_{:04d}".format(randint(0, 10000)).encode('ascii')
    value0 = "test_value_{:04d}".format(randint(0, 10000)).encode('ascii')
    id0 = client_rs.verifiedSet(key0, value0).id
    assert key0 in client_rs.txById(id0)
    assert key0 in client_rs.verifiedTxById(id0)
    assert client_rs.txById(id0 + 100) == None
    assert client_rs.verifiedTxById(id0 + 100) == None
    for i in range(0, 3):
        key = "test_key_{:04d}".format(randint(0, 10000)).encode('ascii')
        value = "test_value_{:04d}".format(randint(0, 10000)).encode('ascii')
        client_rs.verifiedSet(key, value)
    assert key0 in client_rs.verifiedTxById(id0)
