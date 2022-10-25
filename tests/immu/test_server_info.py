import pytest
from immudb import ImmudbClient
from immudb.datatypesv2 import EntriesSpec, EntryTypeAction, EntryTypeSpec
from tests.immuTestClient import ImmuTestClient
def test_server_info(wrappedClient: ImmuTestClient):
    if(not wrappedClient.serverHigherOrEqualsToVersion("1.3.0")):
        pytest.skip("Version of immudb too low")
    client = wrappedClient.client
    response1 = client.serverInfo()
    assert response1.version != ""

    assert len(response1.version) > 0