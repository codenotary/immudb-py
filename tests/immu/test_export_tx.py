import uuid
from immudb import ImmudbClient, datatypesv2
import uuid
import time

def test_export_tx_replicate_tx(client: ImmudbClient):
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
            assert client.get(b'kisz123123kaaaa').value.decode("utf-8") == str(index - 1) 
        client.useDatabase(newuuid1)
    client.useDatabase(newuuid2)
    assert client.get(b'kisz123123kaaaa').value == b'5'