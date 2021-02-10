import pytest
from immudb.client import ImmudbClient
from random import randint
import grpc._channel
import warnings

class TestBasicGetSet:
    def test_no_server(self):
        try:
            a = ImmudbClient("localhost:9999")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pass
        
    def test_basic(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        key="test_key_{:04d}".format(randint(0,10000))
        value="test_value_{:04d}".format(randint(0,10000))

        resp=a.set(key.encode('utf8'),value.encode('utf8'))
        readback=a.get(key.encode('utf8'))
        assert value==readback.value.decode('utf8')
        val=a.getValue(key.encode('utf8'))
        assert val==value.encode('utf-8')
        assert None==a.getValue(b'non_existing_key')
        assert a.healthCheck()
        a.logout()
        a.shutdown()
        
    def test_root(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        r1=a.currentState()
        key="test_key_{:04d}".format(randint(0,10000))
        value="test_value_{:04d}".format(randint(0,10000))
        a.verifiedSet(key.encode('utf8'),value.encode('utf8'))
        r2=a.currentState()
        
        assert r2.id>r1.id
        a.logout()
        a.shutdown()

    def test_property(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        assert isinstance(a.stub,object)
        
    def test_safeSet(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        r1=a.currentState()
        key="test_key_{:04d}".format(randint(0,10000))
        value="test_value_{:04d}".format(randint(0,10000))
        # explicitly test deprecated safeSet
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        a.safeSet(key.encode('utf8'),value.encode('utf8'))
        a.safeGet(key.encode('utf8'))

    def test_get_tx(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        key0="test_key_{:04d}".format(randint(0,10000)).encode('ascii')
        value0="test_value_{:04d}".format(randint(0,10000)).encode('ascii')
        id0=a.verifiedSet(key0,value0).id
        assert key0 in a.txById(id0)
        assert key0 in a.verifiedTxById(id0)
        assert a.txById(id0+100)==None
        assert a.verifiedTxById(id0+100)==None
        for i in range(0,3):
            key="test_key_{:04d}".format(randint(0,10000)).encode('ascii')
            value="test_value_{:04d}".format(randint(0,10000)).encode('ascii')
            a.verifiedSet(key,value)
        assert key0 in a.verifiedTxById(id0)
        
