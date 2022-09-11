from grpc import RpcError
from immudb import ImmudbClient
import pytest
from tests.immuTestClient import ImmuTestClient

def test_flush_index(wrappedClient: ImmuTestClient):
    client = wrappedClient.client
    if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
        pytest.skip("Immudb version too low")
    response1 = client.flushIndex(10.0, True)
    assert response1.database == "defaultdb"
    with pytest.raises(RpcError):
        response1 = client.flushIndex(101.0, True)
        
    with pytest.raises(RpcError):
        response1 = client.flushIndex(-1, True)