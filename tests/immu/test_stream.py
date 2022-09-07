from io import BytesIO
from grpc import RpcError
from immudb import ImmudbClient
import pytest
from immudb.grpc.schema_pb2 import Chunk
from immudb.streamsutils import KeyHeader, ValueChunkHeader

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

def fake_stream():
    ref = BytesIO(('test'*10240).encode("utf-8"))
    yield Chunk(content = KeyHeader(key = b'test', length=4).getInBytes())
    length = ref.getbuffer().nbytes
    firstChunk = ref.read(128)
    firstChunk = ValueChunkHeader(chunk = firstChunk, length = length).getInBytes()
    yield Chunk(content = firstChunk)
    chunk = ref.read(128)
    while chunk:
        yield Chunk(content = chunk)
        chunk = ref.read(128)
        

def test_stream_set(client: ImmudbClient):
    resp = client.streamSet(fake_stream())
    assert resp.id > 0
    assert resp.ts > 0

    assert client.get(b'test').value == ('test'*10240).encode("utf-8")
