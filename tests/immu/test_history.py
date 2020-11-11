import pytest
from immudb.client import ImmudbClient
from random import randint
import grpc._channel

class TestHistory:
        
    def test_history(self):
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        key="history_{:04d}".format(randint(0,9999))
        values=[]
        for i in range(0,10):
            v="value_{:04d}".format(randint(0,9999))
            a.safeSet(key.encode('ascii'),v.encode('ascii'))
            values.append(v)
            
        hh=a.history(key.encode('ascii'))
        assert(len(hh)==10)
        for i in range(0,10):
            assert(hh[i].value==values[9-i].encode('ascii'))
        
