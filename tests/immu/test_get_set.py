import pytest
from immu.client import ImmuClient
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

        a.safeSet(key.encode('utf8'),value.encode('utf8'))
        readback=a.safeGet(key.encode('utf8')).value

        assert value==readback

    def test_get_set_batch(self):
        try:
            a = ImmuClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        xset=[
            {'key':b'gorilla', 'value':b'banana'},
            {'key':b'zebra',   'value':b'grass'},
            {'key':b'lion',    'value':b'zebra'}
            ]
        assert type(a.setAll(xset))!=int
        xget=[x['key'] for x in xset]
        res=a.getAll(xget)
        for i in res.itemlist.items:
            for j in filter(lambda z:z['key']==i.key, xset):
                assert j['value']==i.value.payload



