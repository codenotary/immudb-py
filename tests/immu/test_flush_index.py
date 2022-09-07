from grpc import RpcError
from immudb import ImmudbClient
import pytest

def test_flush_index(client: ImmudbClient):
    response1 = client.flushIndex(10.0, True)
    assert response1.database == "defaultdb"
    with pytest.raises(RpcError):
        response1 = client.flushIndex(101.0, True)
        
    with pytest.raises(RpcError):
        response1 = client.flushIndex(-1, True)