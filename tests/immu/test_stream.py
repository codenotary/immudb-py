from grpc import RpcError
from immudb import ImmudbClient
import pytest
from immudb.streamsutils import KeyHeader

def test_stream_get(client: ImmudbClient):
    key = ('a' * 512).encode('utf-8')
    client.set(key, (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8"))
    stream = client.streamGet(key)
    first = True
    fullValue = b''
    for content in stream:
        if first:
            first = False
            assert type(content) == KeyHeader
            assert content.key == key
            assert content.length == 512
            continue

        fullValue += content.chunk
    assert content.left == 0
    assert len(fullValue) == 1100000 * 2 + 11000 * 2
    assert fullValue == (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")

def test_stream_get_full(client: ImmudbClient):
    key = ('a' * 512).encode('utf-8')
    client.set(key, (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8"))
    kv = client.streamGetFull(key)
    assert len(kv.value) == 1100000 * 2 + 11000 * 2
    assert kv.value == (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")
            