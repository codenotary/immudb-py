import pytest
from immudb.client import ImmudbClient
from random import randint
import grpc._channel
import time
class TestGetSet:

    def test_get_set_uniq(self):
        try:
                a = ImmudbClient("localhost:3322")
                a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
                pytest.skip("Cannot reach immudb server")
        kvs=[]
        for t in range(0,100):
                value="test_value_{:04d}".format(randint(0,10000))
                while True:
                        key="test_key_{:04d}".format(randint(0,10000))
                        if key not in [t[0] for t in kvs]:
                                break
                kvs.append((key,value))
                resp=a.safeSet(key.encode('utf8'),value.encode('utf8'))
                assert resp.verified
        for (key,value) in kvs:
                readback=a.safeGet(key.encode('utf8'))
                print (key,value,readback.value)
                assert readback.verified
                assert value.encode('utf8')==readback.value

    def test_get_set_over(self):
        try:
                a = ImmudbClient("localhost:3322")
                a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
                pytest.skip("Cannot reach immudb server")
        kvs={}
        for t in range(0,300):
                key="test_key_{:04d}".format(randint(0,100))
                value="test_value_{:04d}".format(randint(0,100))
                kvs[key]=value
                resp=a.safeSet(key.encode('utf8'),value.encode('utf8'))
                assert resp.verified
        for (key,value) in kvs.items():
                readback=a.safeGet(key.encode('utf8'))
                print (key,value,readback.value)
                assert readback.verified
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

    def test_get_set_over(self):
        try:
                a = ImmudbClient("localhost:3322")
                a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
                pytest.skip("Cannot reach immudb server")
        r=a.get(b"not:existing:key")
        assert r==None
