from immudb import ImmudbClient
from immudb.datatypesv2 import EntriesSpec, EntryTypeAction, EntryTypeSpec
def test_server_info(client: ImmudbClient):
    response1 = client.serverInfo()
    assert response1.version != ""

    assert len(response1.version) > 0