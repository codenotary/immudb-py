from immudb import ImmudbClient
from immudb.datatypesv2 import EntriesSpec, EntryTypeAction, EntryTypeSpec
def test_tx_scan(client: ImmudbClient):
    response1 = client.set(b"x", b"y")
    response2 = client.set(b"x1", b"y")
    response3 = client.set(b"x2", b"y")
    txId = response3.id
    result = client.txScan(txId, 3)
    assert len(result.txs) == 1
    txId = response2.id
    result = client.txScan(txId, 3)
    assert len(result.txs) == 2
    txId = response1.id
    result = client.txScan(txId, 3)
    assert len(result.txs) == 3