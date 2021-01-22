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
        for t in range(0,100):
            key="test_key_{:04d}".format(randint(0,10000))
            value="test_value_{:04d}".format(randint(0,10000))

            resp=a.safeSet(key.encode('utf8'),value.encode('utf8'))
            assert resp.verified==True
            readback=a.safeGet(key.encode('utf8'))
            assert readback.verified==True
            assert value.encode('utf8')==readback.value

    def test_get_set_batch(self):
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        xset={
            b'gorilla': b'banana',
            b'zebra':   b'grass',
            b'lion':    b'zebra'
            }
        assert type(a.setAll(xset))!=int
        # test getAll
        resp=a.getAll(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i]==resp[i]
        for i in xset.keys():
            assert i in resp
            assert xset[i]==resp[i]
        # test getAllItems
        resp=a.getAll(xset.keys())
        for i in resp.keys():
            assert i in xset
            assert xset[i]==resp[i]



