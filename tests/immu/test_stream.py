from datetime import datetime
from io import BytesIO
import uuid
from grpc import RpcError
from immudb import ImmudbClient, datatypes, datatypesv2
import pytest
from immudb.grpc.schema_pb2 import Chunk
from immudb.streamsutils import KeyHeader, ValueChunkHeader
import random
import string
import tempfile

def test_stream_get_raw(client: ImmudbClient):
    key = ('a' * 512).encode('utf-8')
    client.set(key, (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8"))
    stream = client._rawStreamGet(key)
    first = True
    fullValue = b''
    keyHeader = next(stream)
    assert keyHeader.key == b'a'*512
    assert keyHeader.length == 512
    for content in stream:
        fullValue += content.chunk
    assert content.left == 0
    assert len(fullValue) == 1100000 * 2 + 11000 * 2
    assert fullValue == (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")

def test_stream_get(client: ImmudbClient):
    key = ('x12' * 51).encode('utf-8')
    client.set(key, "ba".encode("utf-8"))
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    readed = buffer.read(10240)
    wholeValue = b''
    while readed:
        wholeValue += readed
        readed = buffer.read(10240)
    assert wholeValue == "ba".encode("utf-8")

    key = ('x12' * 51).encode('utf-8')
    client.set(key, b"ba"*512)
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    readed = buffer.read(10)
    wholeValue = b''
    while readed:
        wholeValue += readed
        readed = buffer.read(10)
    assert len(wholeValue) == len(b"ba"*512)
    assert wholeValue == b"ba"*512

    key = ('x12' * 51).encode('utf-8')
    value = b"one of the test that will test some random words and not generated sequence, so it theoreticaly will lead into edge detection"
    client.set(key, value)
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    readed = buffer.read(1024004040)
    wholeValue = b''
    while readed:
        wholeValue += readed
        readed = buffer.read(1024004040)
    assert len(wholeValue) == len(value)
    assert wholeValue == value

    key = ('x12' * 51).encode('utf-8')
    value = b"one of the test that will test some random words and not generated sequence, so it theoreticaly will lead into edge detection"
    client.set(key, value)
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    readed = buffer.read(1)
    wholeValue = b''
    while readed:
        wholeValue += readed
        readed = buffer.read(1)
    assert len(wholeValue) == len(value)
    assert wholeValue == value
    

    key = ('a' * 512).encode('utf-8')
    value = (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")
    client.set(key, value)
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    readed = buffer.read(1223)
    wholeValue = b''
    while readed:
        wholeValue += readed
        readed = buffer.read(1223)
    assert len(wholeValue) == len(value)
    assert wholeValue == value

def _get_test_data(key: str, value: str, bufferReadLength: int):
    return (
        key.encode("utf-8"),
        value.encode("utf-8"),
        bufferReadLength
    )

testdata = [
    _get_test_data("1", "1", 10240123),
    _get_test_data("1", "1", 1),
    _get_test_data("1", "1", 2),
    _get_test_data("asd"*128, "1", 2),
    _get_test_data("asd"*128, "12"*128, 2),
    _get_test_data("asd"*128, "13"*128, 1),
    _get_test_data("asd"*128, "31"*128, 1024*64),
    _get_test_data("1"*100, "123"*1024*1024, 1024*64),
    _get_test_data("asd"*128, "asd"*1024*1024, 1024*64),
    _get_test_data("asd"*128, "dsadsadsdsa"*1024*1024, 1024),
    _get_test_data("asd"*128, "dasdad"*1024*1024, 1024*640),
    _get_test_data("asd"*128, "dasdsdsa"*1024*1024, 1024*6400000),
    _get_test_data("asd"*128, "1sadsads23"*1024*1024, 65528), # exact chunk size - 8 (first 8 bytes)
    _get_test_data("x4"*128, "1sadsads23"*1024*1024, 65535), # exact chunk size - 1
    _get_test_data("x3"*128, "12adsdsads3"*1024*1024, 65537), # exact chunk size + 1
    _get_test_data("x2"*128, "12adsdsads3"*1024*1024, 65536), # exact chunk size 
    _get_test_data("x2"*128, "12adsdsads3"*1024*1024, 65636), # exact chunk size + 100
    _get_test_data("x1"*128, "b"*33554431, 1024*64), # 31MB case < 32MB, max immudb constraint 33554432
]

def determineTestId(val):
    if isinstance(val, bytes):
        return len(val)
    elif isinstance(val, int):
        return str(val)
    else:
        return val

@pytest.mark.parametrize("key,value,bufferReadLength", testdata, ids = determineTestId)
def test_stream_get_multiple_cases(client: ImmudbClient, key: bytes, value: bytes, bufferReadLength: int):
    client.streamSet(key, BytesIO(value), len(value))
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    assert len(buffer) == len(value)
    assert buffer.size == len(value)
    readed = buffer.read(bufferReadLength)
    wholeValue = b''
    while readed:
        wholeValue += readed
        readed = buffer.read(bufferReadLength)
    assert len(wholeValue) == len(value)
    assert wholeValue == value


bigTestData = [
    _get_test_data("asd"*128, "1sadsads23"*1024*1024, 65528), # exact chunk size - 8 (first 8 bytes)
    _get_test_data("x4"*128, "1sadsads23"*1024*1024, 65535), # exact chunk size - 1
    _get_test_data("x3"*128, "12adsdsads3"*1024*1024, 65537), # exact chunk size + 1
    _get_test_data("x2"*128, "12adsdsads3"*1024*1024, 65536), # exact chunk size 
    _get_test_data("x2"*128, "12adsdsads3"*1024*1024, 65636), # exact chunk size + 100
    _get_test_data("x1"*128, "b"*33554431, 1024*64), # 31MB case < 32MB, max immudb constraint 33554432
]

@pytest.mark.parametrize("key,value,bufferReadLength", bigTestData, ids = determineTestId)
def test_stream_close_multiple_cases(client: ImmudbClient, key: bytes, value: bytes, bufferReadLength: int):
    client.streamSet(key, BytesIO(value), len(value))
    keyFrom, buffer = client.streamGet(key)
    assert keyFrom == key
    assert len(buffer) == len(value)
    assert buffer.size == len(value)
    
    readed = buffer.read(bufferReadLength)
    # Close during running process
    buffer.close()
    with pytest.raises(RpcError): # Buffer closed
        readed = buffer.read(bufferReadLength)



def determineTestId(val):
    if isinstance(val, bytes):
        return len(val)
    elif isinstance(val, int):
        return str(val)
    else:
        return val

def _get_test_data_set(key, value, chunksize):
    return key.encode("utf-8"), BytesIO(value.encode("utf-8")), chunksize

testdata_set = [
    _get_test_data_set("1", "1", 10240123),
    _get_test_data_set("1", "1", 1),
    _get_test_data_set("1", "1", 2),
    _get_test_data_set("asd"*128, "1", 2),
    _get_test_data_set("asd"*128, "12"*128, 2),
    _get_test_data_set("asd"*128, "13"*128, 1),
    _get_test_data_set("asd"*128, "31"*128, 1024*64),
    _get_test_data_set("1"*100, "123"*1024*1024, 1024*64),
    _get_test_data_set("asd"*128, "asd"*1024*1024, 1024*64),
    _get_test_data_set("asd"*128, "dsadsadsdsa"*1024*1024, 1024),
    _get_test_data_set("asd"*128, "dasdad"*1024*1024, 1024*640),
    _get_test_data_set("asd"*128, "dasdsdsa"*1024*1024, 1024*6400000),
    _get_test_data_set("asd"*128, "1sadsads23"*1024*1024, 65528), # exact chunk size - 8 (first 8 bytes)
    _get_test_data_set("x4"*128, "1sadsads23"*1024*1024, 65535), # exact chunk size - 1
    _get_test_data_set("x3"*128, "12adsdsads3"*1024*1024, 65537), # exact chunk size + 1
    _get_test_data_set("x2"*128, "12adsdsads3"*1024*1024, 65536), # exact chunk size 
    _get_test_data_set("x2"*128, "12adsdsads3"*1024*1024, 65636), # exact chunk size + 100
    _get_test_data_set("x1"*128, "b"*33554431, 1024*64), # 31MB case < 32MB, max immudb constraint 33554432
]

@pytest.mark.parametrize("key,value,chunkSize", testdata_set, ids = determineTestId)
def test_stream_set_multiple_cases(client: ImmudbClient, key: bytes, value: BytesIO, chunkSize: int):
    txHeader = client.streamSet(key, value, value.getbuffer().nbytes, chunkSize)
    assert txHeader.id > 0
    assert txHeader.nentries == 1

    txHeader = client.streamSetFullValue(key, value.getvalue(), chunkSize)
    assert txHeader.id > 0
    assert txHeader.nentries == 1
    
    
        
def test_stream_scan(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), b'test')
    client.streamSetFullValue((keyprefix + "Y").encode("utf-8"), b'test')
    entries = client.streamScan(prefix = keyprefix.encode("utf-8"))
    kv = next(entries)
    assert kv.key == (keyprefix + "X").encode("utf-8")
    assert kv.value == b'test'

    kv = next(entries)
    assert kv.key == (keyprefix + "Y").encode("utf-8")
    assert kv.value == b'test'

    with pytest.raises(StopIteration):
        kv = next(entries)

    for kv in client.streamScan(prefix = keyprefix.encode("utf-8")):
        assert kv.key.decode("utf-8").startswith(keyprefix)
        assert kv.value == b'test'  

def test_stream_scan_one(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), b'test')
    entries = client.streamScan(prefix = keyprefix.encode("utf-8"))
    kv = next(entries)
    assert kv.key == (keyprefix + "X").encode("utf-8")
    assert kv.value == b'test'

    with pytest.raises(StopIteration):
        kv = next(entries)

    for kv in client.streamScan(prefix = keyprefix.encode("utf-8")):
        assert kv.key.decode("utf-8").startswith(keyprefix)
        assert kv.value == b'test'

def test_stream_scan_big(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    value = b'xab' * 1024 * 1024
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), value)
    entries = client.streamScan(prefix = keyprefix.encode("utf-8"))
    kv = next(entries)
    assert kv.key == (keyprefix + "X").encode("utf-8")
    assert kv.value == value

    with pytest.raises(StopIteration):
        kv = next(entries)

    for kv in client.streamScan(prefix = keyprefix.encode("utf-8")):
        assert kv.key.decode("utf-8").startswith(keyprefix)
        assert kv.value == value

def test_stream_scan_big_multiple(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    value = b'xab' * 1024 * 1024 * 10
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), value)
    client.streamSetFullValue((keyprefix + "Y").encode("utf-8"), value)
    client.streamSetFullValue((keyprefix + "Z").encode("utf-8"), value)
    client.streamSetFullValue((keyprefix + "A").encode("utf-8"), value)
    client.streamSetFullValue((keyprefix + "B").encode("utf-8"), value)
    entries = client.streamScan(prefix = keyprefix.encode("utf-8"))

    for kv in entries:
        assert kv.key.decode("utf-8").startswith(keyprefix)
        assert kv.value == value

    toList = list(client.streamScan(prefix = keyprefix.encode("utf-8")))
    assert len(toList) == 5
    assert datatypesv2.KeyValue(key = (keyprefix + "A").encode("utf-8"), value = value) in toList
    assert datatypesv2.KeyValue(key = (keyprefix + "B").encode("utf-8"), value = value) in toList
    assert datatypesv2.KeyValue(key = (keyprefix + "X").encode("utf-8"), value = value) in toList
    assert datatypesv2.KeyValue(key = (keyprefix + "Y").encode("utf-8"), value = value) in toList
    assert datatypesv2.KeyValue(key = (keyprefix + "Z").encode("utf-8"), value = value) in toList

def test_stream_scan_chunked(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), b'test0')
    client.streamSetFullValue((keyprefix + "Y").encode("utf-8"), b'test1')
    client.streamSetFullValue((keyprefix + "Z").encode("utf-8"), b'test2')
    client.streamSetFullValue((keyprefix + "A").encode("utf-8"), b'test3')
    client.streamSetFullValue((keyprefix + "B").encode("utf-8"), b'test4')
    entries = client.streamScanBuffered(prefix = keyprefix.encode("utf-8"))
    index = 0
    for key, buffer in entries:
        index += 1
        fullValue = b''
        readed = buffer.read(512)
        while readed:
            fullValue += readed
            readed = buffer.read(512)   
        assert fullValue.decode("utf-8").startswith("test")
        assert len(fullValue) == 5
        assert key.decode("utf-8").startswith(keyprefix)
    assert index == 5

corner_cases_scan_chunked = [
    (1024, 1, 1),
    (1, 1024, 1),
    (100, 1024*1024, 100),
    (13, 123123, 999),
    (3333, 55, 17),
    (13333, None, 3),
    (1231321, 1111, 99),
    (2, None, 99),
    (13333333, 102400, 11),
    (11, 1231, 8),
    (3331, 1024, 99),
    (1111, 1024, 99),
    (1111, 11, 10),

]

@pytest.mark.parametrize("valueSize,readSize,howMuch", corner_cases_scan_chunked, ids = determineTestId)
def test_stream_scan_chunked_corner_cases(client: ImmudbClient, valueSize, readSize, howMuch):
    keyprefix = str(uuid.uuid4())
    letters = string.ascii_lowercase
    toFound = dict()
    found = dict()
    for i in range(0, howMuch):
        generated = ''.join(random.choice(letters) for i in range(valueSize)).encode("utf-8")
        toFound[f"{keyprefix}{i}".encode("utf-8")] = generated
        client.streamSetFullValue(f"{keyprefix}{i}".encode("utf-8"), generated)

    entries = client.streamScanBuffered(prefix = keyprefix.encode("utf-8"))
    index = 0
    for key, buffer in entries:
        index += 1
        fullValue = b''
        readed = buffer.read(readSize)
        while readed:
            fullValue += readed
            readed = buffer.read(readSize)
        assert toFound[key] == fullValue
        found[key] = True
    
    for key, value in toFound.items():
        assert found[key] == True


    assert index == howMuch

def test_stream_scan_chunked_big(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    expectedLength = 5 * 1024 * 1024
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), b'test0' * 1024 * 1024)
    client.streamSetFullValue((keyprefix + "Y").encode("utf-8"), b'test1' * 1024 * 1024)
    client.streamSetFullValue((keyprefix + "Z").encode("utf-8"), b'test2' * 1024 * 1024)
    client.streamSetFullValue((keyprefix + "A").encode("utf-8"), b'test3' * 1024 * 1024)
    client.streamSetFullValue((keyprefix + "B").encode("utf-8"), b'test4' * 1024 * 1024)
    entries = client.streamScanBuffered(prefix = keyprefix.encode("utf-8"))
    index = 0
    for key, buffer in entries:
        index += 1
        fullValue = b''
        readed = buffer.read(512)
        while readed:
            fullValue += readed
            readed = buffer.read(512)   
        assert fullValue.decode("utf-8").startswith("test")
        assert len(fullValue) == expectedLength
        assert key.decode("utf-8").startswith(keyprefix)
    assert index == 5

def test_stream_scan_chunked_one(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), b'test0')
    entries = client.streamScanBuffered(prefix = keyprefix.encode("utf-8"))
    index = 0
    for key, buffer in entries:
        index += 1
        assert key == (keyprefix + "X").encode("utf-8")
        fullValue = b''
        readed = buffer.read(512)
        while readed:
            fullValue += readed
            readed = buffer.read(512)   
        assert fullValue.decode("utf-8").startswith("test")
        assert len(fullValue) == 5
    assert index == 1

def test_stream_redirect(client: ImmudbClient):
    keyprefix = str(uuid.uuid4())
    newKeyPrefix = str(uuid.uuid4())
    client.streamSetFullValue((keyprefix + "X").encode("utf-8"), b'test0')
    client.streamSetFullValue((keyprefix + "Y").encode("utf-8"), b'test0')
    client.streamSetFullValue((keyprefix + "Z").encode("utf-8"), b'test0')
    entries = client.streamScanBuffered(prefix = keyprefix.encode("utf-8"))
    index = 0
    for key, buffer in entries:
        index += 1
        toRedirect = newKeyPrefix.encode("utf-8") + str(index).encode("utf-8")
        client.streamSet(toRedirect, buffer, buffer.size) # Simulates redirection from one stream to another 

        keyFrom, bufferFrom = client.streamGet(toRedirect)
        readed = bufferFrom.read() # Reads full value
        assert readed == b'test0'

    assert index == 3
        

def test_stream_get_full(client: ImmudbClient):
    key = ('a' * 512).encode('utf-8')
    client.set(key, (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8"))

    kv = client.streamGetFull(key)

    assert len(kv.value) == 1100000 * 2 + 11000 * 2
    assert kv.value == (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")

    client.setReference(key, b'superref')
    kv = client.streamGetFull(b'superref')

    assert len(kv.value) == 1100000 * 2 + 11000 * 2
    assert kv.key == key
    assert kv.value == (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")

def test_stream_read_full(client: ImmudbClient):
    key = ('a' * 512).encode('utf-8')
    client.set(key, (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8"))

    keyFrom, buffer = client.streamGet(key)
    value = buffer.read()
    assert key == keyFrom

    assert len(value) == 1100000 * 2 + 11000 * 2
    assert value == (('xa' * 11000) + ('ba' * 1100000)).encode("utf-8")

def fake_stream(length = 10240):
    ref = BytesIO(('test'*length).encode("utf-8"))
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
    resp = client._rawStreamSet(fake_stream())
    assert resp.id > 0
    assert resp.ts > 0

    assert client.get(b'test').value == ('test'*10240).encode("utf-8")


def test_stream_history(client: ImmudbClient):
    key = str(uuid.uuid4()).encode("utf-8")
    client.set(key, b'1')
    client.set(key, b'2')
    client.set(key, b'3')
    resp = client.streamHistory(key = key)
    value = next(resp)
    assert value.key == key
    assert value.value == b'1'
    value = next(resp)
    assert value.key == key
    assert value.value == b'2'
    value = next(resp)
    assert value.key == key
    assert value.value == b'3'
    with pytest.raises(StopIteration):
        value = next(resp)

def test_stream_history_buffered(client: ImmudbClient):
    key = str(uuid.uuid4()).encode("utf-8")
    client.set(key, b'1'* 300)
    client.set(key, b'2'* 300)
    client.set(key, b'3'* 300)
    resp = client.streamHistoryBuffered(key)
    index = 1
    for keyNow, bufferedReader in resp:
        assert keyNow == key
        assert bufferedReader.read(256) == str(index).encode("utf-8") * 256 # First 256
        assert bufferedReader.read(256) == str(index).encode("utf-8") * 44 # Last 44 
        index += 1


def test_stream_zscan(client: ImmudbClient):
    key = str(uuid.uuid4()).encode("utf-8")
    key2 = str(uuid.uuid4()).encode("utf-8")
    key3 = str(uuid.uuid4()).encode("utf-8")
    key4 = str(uuid.uuid4()).encode("utf-8")
    set = b'set' + str(uuid.uuid4()).encode("utf-8")
    client.set(key, b'b'*356)
    client.set(key2, b'b'*356)
    client.set(key3, b'b'*356)
    client.set(key4, b'b'*356)
    client.zAdd(set, 4.0, key)
    client.zAdd(set, 3.0, key2)
    client.zAdd(set, 5.0, key3)
    client.zAdd(set, 6.0, key4)
    resp = client.streamZScan(set = set, limit = 2, minScore=3.5)
    index = 0
    toFind = 4.0
    lastFind = None
    for item in resp:
        assert item.score == toFind
        assert item.value == b'b' * 356
        assert len(item.key) == 36
        toFind += 1
        index += 1
        lastFind = item.score

    assert lastFind == 5.0 # limit = 2, minScore = 3.5

    assert index == 2


def test_stream_zscan_buffered(client: ImmudbClient):
    key = str(uuid.uuid4()).encode("utf-8")
    key2 = str(uuid.uuid4()).encode("utf-8")
    key3 = str(uuid.uuid4()).encode("utf-8")
    key4 = str(uuid.uuid4()).encode("utf-8")

    set = b'set' + str(uuid.uuid4()).encode("utf-8")
    client.set(key, b'b'*356)
    client.set(key2, b'b'*356)
    client.set(key3, b'b'*356)
    client.set(key4, b'b'*356)
    client.zAdd(set, 4.0, key)
    client.zAdd(set, 3.0, key2)
    client.zAdd(set, 5.0, key3)
    client.zAdd(set, 6.0, key4)
    resp = client.streamZScanBuffered(set = set, limit = 2, minScore=3.5)
    index = 0
    toFind = 4.0
    lastFind = None
    for item, reader in resp:
        assert item.score == toFind
        assert reader.read() == b'b' * 356
        assert len(item.key) == 36
        toFind += 1
        index += 1
        lastFind = item.score

    assert index == 2
    assert lastFind == 5.0 # limit = 2, minScore = 3.5


def test_stream_verifiable_get(client: ImmudbClient):
    key = str(uuid.uuid4()).encode("utf-8")
    
    client.set(key, b'test1')
    client.set(key, b'test2'*1024)
    client.setReference(key, b'ref1')
    client.setReference(key, b'ref2')
    resp = client.streamVerifiedGet(key = b'ref2')
    
    assert resp.key == key
    assert resp.verified == True
    assert resp.value == b'test2'*1024
    assert resp.refkey == b'ref2'

    resp = client.streamVerifiedGet(key = key)
    
    assert resp.key == key
    assert resp.verified == True
    assert resp.value == b'test2'*1024
    assert resp.refkey == None

def test_stream_verifiable_get_buffered(client: ImmudbClient):
    key = str(uuid.uuid4()).encode("utf-8")
    
    client.set(key, b'test1')
    client.set(key, b'test2'*1024)
    client.setReference(key, b'ref1')
    client.setReference(key, b'ref2')
    resp, value = client.streamVerifiedGetBuffered(key = b'ref2')
    
    assert resp.key == key
    assert resp.verified == True
    assert value.read() == b'test2'*1024
    assert resp.refkey == b'ref2'


    resp, value = client.streamVerifiedGetBuffered(key = key)
    
    assert resp.key == key
    assert resp.verified == True
    assert value.read() == b'test2'*1024
    assert resp.refkey == None



def test_verifiable_stream_set(client: ImmudbClient):
    keyToSet = str(uuid.uuid4()).encode("utf-8")
    kk = BytesIO(b'123123')
    resp = client.streamVerifiedSet(keyToSet, kk, 6, 100)
    assert resp.id > 0
    assert resp.verified == True

    assert client.get(keyToSet).value == b'123123'

    keyToSet = str(uuid.uuid4()).encode("utf-8")
    kk = BytesIO(b'123123'*1024)
    resp = client.streamVerifiedSet(keyToSet, kk, 6*1024, 100)
    assert resp.id > 0
    assert resp.verified == True

    assert client.get(keyToSet).value == b'123123'*1024


def test_verifiable_stream_set_fullvalue(client: ImmudbClient):
    keyToSet = str(uuid.uuid4()).encode("utf-8")
    resp = client.streamVerifiedSetFullValue(keyToSet, b'123123', 100)
    assert resp.id > 0
    assert resp.verified == True

    assert client.get(keyToSet).value == b'123123'

    keyToSet = str(uuid.uuid4()).encode("utf-8")
    resp = client.streamVerifiedSetFullValue(keyToSet, b'123123'*1024, 100)
    assert resp.id > 0
    assert resp.verified == True

    assert client.get(keyToSet).value == b'123123'*1024

def test_stream_exec_all(client: ImmudbClient):
    keyToSet = str(uuid.uuid4()).encode("utf-8")
    keyToSet2 = str(uuid.uuid4()).encode("utf-8")
    keyToSet3 = str(uuid.uuid4()).encode("utf-8")
    val = b'123'*80000
    val2 = b'321'*80000
    resp = client.streamExecAll([
        datatypes.KeyValue(
            key=keyToSet,
            value=val
        ),
        datatypes.StreamingKeyValue(key = keyToSet2,
            value = BytesIO(val2),
            length = len(val)
        )
    ])
    k1 = client.get(keyToSet)
    assert k1.value == val
    k2 = client.get(keyToSet2)
    assert k2.value == val2

def test_stream_exec_all_zadd(client: ImmudbClient):
    keyToSet = str(uuid.uuid4()).encode("utf-8")
    keyToSet2 = str(uuid.uuid4()).encode("utf-8")
    keyToSet3 = str(uuid.uuid4()).encode("utf-8")
    val = b'123'*80000
    val2 = b'321'*80000
    resp = client.streamExecAll([
        datatypes.KeyValue(
            key=keyToSet,
            value=val
        ),
        datatypes.StreamingKeyValue(key = keyToSet2,
            value = BytesIO(val2),
            length = len(val)
        )
    ])
    k1 = client.get(keyToSet)
    assert k1.value == val
    k2 = client.get(keyToSet2)
    assert k2.value == val2


    keyToSet = str(uuid.uuid4()).encode("utf-8")
    keyToSet2 = str(uuid.uuid4()).encode("utf-8")
    keyToSet3 = str(uuid.uuid4()).encode("utf-8")

    setToSet = b"SET" + str(uuid.uuid4()).encode("utf-8")

    val = b'123'*80000
    val2 = b'321'*80000
    resp = client.streamExecAll([
        datatypes.KeyValue(
            key=keyToSet,
            value=val
        ),
        datatypes.StreamingKeyValue(key = keyToSet2,
            value = BytesIO(val2),
            length = len(val)
        ),
        datatypes.ZAddRequest(set = setToSet, score = 3.0, key = keyToSet)
    ])
    k1 = client.get(keyToSet)
    assert k1.value == val
    k2 = client.get(keyToSet2)
    assert k2.value == val2

    k3 = list(client.streamZScan(setToSet))
    assert len(k3) == 1
    assert k3[0].score == 3.0
    assert k3[0].value == val
    assert k3[0].key == keyToSet