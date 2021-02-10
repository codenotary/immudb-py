import pytest
from immudb.client import ImmudbClient
import random,string
import grpc._channel

def get_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for i in range(length))

        
def test_reference():
    try:
        a = ImmudbClient("localhost:3322")
        a.login("immudb","immudb")
    except grpc._channel._InactiveRpcError as e:
        pytest.skip("Cannot reach immudb server")
    k="reftest.key."+get_random_string(16)
    v=get_random_string(32)
    r1="reftest.reference."+get_random_string(16)
    r2="reftest.reference."+get_random_string(16)
    setresp=a.verifiedSet(k.encode('ascii'),v.encode('ascii'))
    a.setReference(k.encode('ascii'),r1.encode('ascii'))
    a.verifiedSetReference(k.encode('ascii'),r2.encode('ascii'))
    referred1=a.verifiedGet(r1.encode('ascii'))
    referred2=a.verifiedGet(r1.encode('ascii'))
    original=a.verifiedGet(k.encode('ascii'))
    assert original.key==referred1.key
    assert original.value==referred1.value
    assert original.key==referred2.key
    assert original.value==referred2.value
