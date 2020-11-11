import pytest
from immudb.client import ImmudbClient
from random import randint
import grpc._channel

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
  
        a.logout()
        a.shutdown()
        
    def test_root(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        r1=a.currentRoot()
        key="test_key_{:04d}".format(randint(0,10000))
        value="test_value_{:04d}".format(randint(0,10000))
        a.safeSet(key.encode('utf8'),value.encode('utf8'))
        r2=a.currentRoot()
        
        assert r2.index>r1.index
        a.logout()
        a.shutdown()

    def test_property(self):
        try:
            a = ImmudbClient()
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        assert isinstance(a.stub,object)
        
