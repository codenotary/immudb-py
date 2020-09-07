import pytest
from immu.client import ImmuClient
from random import randint
import grpc._channel

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

