import pytest
from immudb.client import ImmuClient
from random import randint
import grpc._channel

class TestGetSet:
        
    def test_get_set(self):
        try:
            a = ImmuClient("localhost:3322")
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

    def test_get_set_batch(self):
        try:
            a = ImmuClient("localhost:3322")
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
        resp=a.getAllItems(xset.keys())
        for i in resp.itemlist.items:
            assert i.key in xset
            assert xset[i.key]==i.value.payload



