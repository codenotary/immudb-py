from grpc import RpcError
from immudb import ImmudbClient
import pytest
from immudb.streamsutils import KeyHeader



def test_stream_get_full(client: ImmudbClient):
    key = ('a' * 512).encode('utf-8')
    client.set(key, (('x' * 33544432)).encode("utf-8"))
    with pytest.raises(RpcError):
        client.get(key) # Error because length too big - client side

    client = ImmudbClient(client._url, rs = client._rs, max_grpc_message_length=200 * 1024 * 1024)
    client.login("immudb", "immudb")
    client.set(key, (('x' * 33544432)).encode("utf-8")) # ~32 mb
    client.get(key)

            