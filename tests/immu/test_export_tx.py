import uuid
from immudb import ImmudbClient, datatypesv2
import uuid
import time
from tests.immuTestClient import ImmuTestClient
import pytest

def test_export_tx_replicate_tx(wrappedClient: ImmudbClient):
    client = wrappedClient.client
    if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
        pytest.skip("Immudb version too low")
    if wrappedClient.serverHigherOrEqualsToVersion("1.5.0"):
        offset = 0
    else:
        offset = 1

    newuuid1 = str(uuid.uuid4()).replace("-", "")
    client.createDatabaseV2(newuuid1, settings = datatypesv2.DatabaseSettingsV2(
    ), ifNotExists=False)

    newuuid2 = str(uuid.uuid4()).replace("-", "")
    client.createDatabaseV2(newuuid2, settings = datatypesv2.DatabaseSettingsV2(
        replicationSettings=datatypesv2.ReplicationSettings(
            replica = True,
        )
    ), ifNotExists=False)
    client.useDatabase(newuuid1)
    tx = client.set(b'kisz123123kaaaa', b'1')
    tx = client.set(b'kisz123123kaaaa', b'2')
    tx = client.set(b'kisz123123kaaaa', b'3')
    tx = client.set(b'kisz123123kaaaa', b'4')
    tx = client.set(b'kisz123123kaaaa', b'5')
    for index in range(1, tx.id + 1):
        resp = client.exportTx(index)
        client.useDatabase(newuuid2)
        txHeader = client.replicateTx(resp)
        assert txHeader.id > 0
        assert txHeader.nentries == 1
        if(index > 1): # First transaction is not db set
            assert client.get(b'kisz123123kaaaa').value.decode("utf-8") == str(index - offset)
        client.useDatabase(newuuid1)
    client.useDatabase(newuuid2)
    assert client.get(b'kisz123123kaaaa').value == b'5'
