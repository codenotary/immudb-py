import pytest
from immudb.client import ImmudbClient
from random import randint
import grpc._channel

class TestGetSet:
        
    def test_get_set(self):
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        key="test_key_{:04d}".format(randint(0,10000))
        value="test_value_{:04d}".format(randint(0,10000))

        resp=a.safeSet(key.encode('utf8'),value.encode('utf8'))
        assert resp.verified==True
        readback=a.safeGet(key.encode('utf8'))
        assert readback.verified==True
        assert value==readback.value



