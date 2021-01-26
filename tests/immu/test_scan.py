import pytest
from immudb.client import ImmudbClient
import random,string
import grpc._channel

def get_random_string(length):
    return ''.join(random.choice(string.printable) for i in range(length))

        
def test_scan_set():
    try:
        a = ImmudbClient("localhost:3322")
        a.login("immudb","immudb")
    except grpc._channel._InactiveRpcError as e:
        pytest.skip("Cannot reach immudb server")
    xset={}
    for i in range(0,100):
        xset["scan:{:04X}".format(i).encode('utf8')]=get_random_string(32).encode('utf8')            
    ret=a.setAll(xset)
    off=None
    kv=a.scan(None, b"scan:",False,17,ret.id)
    while len(kv)>0:
        if len(kv)==0:
            break
        for k in kv:
            print(k,kv[k],xset[k])
            assert kv[k]==xset[k]
            del xset[k]
            off=k
        kv=a.scan(off, b"scan:",False,17)
    assert len(xset)==0

if __name__=="__main__":
    test_scan_set()
