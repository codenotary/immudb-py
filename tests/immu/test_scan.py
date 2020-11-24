import pytest
from immudb.client import ImmudbClient
import random,string
import grpc._channel

def get_random_string(length):
    return ''.join(random.choice(string.printable) for i in range(length))

class TestScan:
        
    def test_scan_set(self):
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        xset={}
        for i in range(0,10):
            xset["scan:{:04X}".format(i).encode('utf8')]=get_random_string(32).encode('utf8')            
        assert type(a.setAll(xset))!=int
        kv=a.scan(b"scan:",None)
        for k in kv:
            assert kv[k]==xset[k]
