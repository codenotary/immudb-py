import pytest
from immu.client import ImmuClient
import string,random
import grpc._channel



def get_random_string(length):
    return ''.join(random.choice(string.printable) for i in range(length))

class TestGetSet:
    def test_get_set_massive(self):
        try:
            a = ImmuClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        xset={}
        for i in range(0,10000):
            xset["massif:{:04X}".format(i).encode('utf8')]=get_random_string(32).encode('utf8')
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



