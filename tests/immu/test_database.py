import pytest
from immu.client import ImmuClient
from random import randint
import grpc._channel
import google.protobuf.empty_pb2

class TestDatabase:

    def test_list_use(self):
        try:
            a = ImmuClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        resp=a.databaseList()
        assert resp.dblist.databases[0].databasename=="defaultdb"
        resp=a.databaseUse(b"defaultdb")
        assert type(resp.reply.token)==str
        
        # create a new DB with a random name (must be lowercase)
        newdb="testdb{:04x}".format(randint(0,65536)).encode('utf8')
        resp=a.databaseCreate(newdb)
        assert type(resp.reply)== google.protobuf.empty_pb2.Empty
        # try and use the new DB
        resp=a.databaseUse(newdb)
        assert type(resp.reply.token)==str

        key="test_key_{:04d}".format(randint(0,10000))
        value="test_value_{:04d}".format(randint(0,10000))

        resp=a.safeSet(key.encode('utf8'),value.encode('utf8'))
        assert resp.verified==True
        readback=a.safeGet(key.encode('utf8'))
        assert readback.verified==True
        assert value==readback.value
        
        

