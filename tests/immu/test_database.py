import pytest
from immudb.client import ImmudbClient
from random import randint
import grpc._channel
import google.protobuf.empty_pb2


class TestDatabase:

    def test_list_use(self, client):
        resp = client.databaseList()
        assert "defaultdb" in resp
        resp = client.useDatabase(b"defaultdb")
        assert type(resp.reply.token) == str

        # create a new DB with a random name (must be lowercase)
        newdb = "testdb{:04x}".format(randint(0, 65536)).encode('utf8')
        resp = client.createDatabase(newdb)
        assert type(resp.reply) == google.protobuf.empty_pb2.Empty
        # try and use the new DB
        resp = client.useDatabase(newdb)
        assert type(resp.reply.token) == str

        key = "test_key_{:04d}".format(randint(0, 10000))
        value = "test_value_{:04d}".format(randint(0, 10000))

        resp = client.verifiedSet(key.encode('utf8'), value.encode('utf8'))
        assert resp.verified == True
        readback = client.verifiedGet(key.encode('utf8'))
        assert readback.verified == True
        assert value.encode('utf8') == readback.value

        deprecatedDb = "deprecateddb{:04x}".format(
            randint(0, 65536)).encode('utf8')
        with pytest.deprecated_call():
            resp = client.databaseCreate(deprecatedDb)
        assert type(resp.reply) == google.protobuf.empty_pb2.Empty

        with pytest.deprecated_call():
            resp = client.databaseUse(deprecatedDb)
        assert type(resp.reply.token) == str
