
from dataclasses import dataclass
import struct
from .grpc.schema_pb2 import VerifiableTx, Entry
from .grpc.schema_pb2 import InclusionProof


@dataclass
class KeyHeader:
    key: bytes
    length: int
    refKey: bytes = None
    refKeyTx: int = None
    tx: int = None

    def getInBytes(self):
        return self.length.to_bytes(8, 'big') + self.key


@dataclass
class ProvenSinceHeader:
    provenSinceTx: int

    def getInBytes(self):
        toBytes = self.provenSinceTx.to_bytes(8, 'big')
        length2 = int.to_bytes(8, 8, 'big')
        return length2 + toBytes


@dataclass
class SetHeader:
    set: bytes
    length: int

    def getInBytes(self):
        return self.length.to_bytes(8, 'big') + self.set


@dataclass
class ScoreHeader:
    score: float


@dataclass
class AtTXHeader:
    seenAtTx: int


@dataclass
class ValueChunkHeader:
    chunk: bytes
    length: int

    def getInBytes(self):
        return self.length.to_bytes(8, 'big') + self.chunk


@dataclass
class ValueChunk:
    chunk: bytes
    left: int


@dataclass
class FullKeyValue:
    key: bytes
    value: bytes


class VerifiedGetStreamReader:
    def __init__(self, stream):
        self.streamToRead = stream
        self.reader = self.headerReader
        self.valueLength = -1
        self.left = -1

    def parseVerifiableTx(self, header):
        verifiable = VerifiableTx()
        verifiable.ParseFromString(header[8:])
        return verifiable

    def parseInclusionProof(self, header):
        inclusion = InclusionProof()
        inclusion.ParseFromString(header[8:])
        return inclusion

    def parseHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        en = Entry()
        en.ParseFromString(header[8:])
        refkey = en.referencedBy.key
        if refkey == b'':
            refkey = None
        return KeyHeader(length=length, key=en.key, refKey=refkey, refKeyTx=en.referencedBy.tx, tx=en.tx)

    def parseValueHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        self.valueLength = length
        self.left = self.valueLength - len(header[8:])
        return ValueChunk(chunk=header[8:], left=self.left)

    def chunks(self):
        for chunk in self.streamToRead:
            yield self.reader(chunk)

    def headerReader(self, chunk):
        self.reader = self.verifiableTxReader
        return self.parseHeader(chunk.content)

    def valueHeaderReader(self, chunk):
        self.reader = self.valueReader
        readed = self.parseValueHeader(chunk.content)
        if (self.left == 0):
            self.reader = self.headerReader
        return readed

    def valueReader(self, chunk):
        self.left = self.left - len(chunk.content)
        readed = ValueChunk(chunk=chunk.content, left=self.left)
        if (self.left == 0):
            self.reader = self.headerReader
        return readed

    def verifiableTxReader(self, chunk):
        self.reader = self.inclusionProofReader
        return self.parseVerifiableTx(chunk.content)

    def inclusionProofReader(self, chunk):
        self.reader = self.valueHeaderReader
        return self.parseInclusionProof(chunk.content)


class StreamReader:
    def __init__(self, stream):
        self.streamToRead = stream
        self.reader = self.headerReader
        self.valueLength = -1
        self.left = -1

    def parseHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        return KeyHeader(length=length, key=header[8:])

    def parseValueHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        self.valueLength = length
        self.left = self.valueLength - len(header[8:])
        return ValueChunk(chunk=header[8:], left=self.left)

    def chunks(self):
        for chunk in self.streamToRead:
            yield self.reader(chunk)

    def headerReader(self, chunk):
        self.reader = self.valueHeaderReader
        return self.parseHeader(chunk.content)

    def valueHeaderReader(self, chunk):
        self.reader = self.valueReader
        readed = self.parseValueHeader(chunk.content)
        if (self.left == 0):
            self.reader = self.headerReader
        return readed

    def valueReader(self, chunk):
        self.left = self.left - len(chunk.content)
        readed = ValueChunk(chunk=chunk.content, left=self.left)
        if (self.left == 0):
            self.reader = self.headerReader
        return readed


class ZScanStreamReader:
    def __init__(self, stream):
        self.streamToRead = stream
        self.reader = self.setHeaderReader
        self.valueLength = -1
        self.left = -1

    def parseHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        return KeyHeader(length=length, key=header[8:])

    def parseSetHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        return SetHeader(length=length, set=header[8:])

    def parseValueHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        self.valueLength = length
        self.left = self.valueLength - len(header[8:])
        return ValueChunk(chunk=header[8:], left=self.left)

    def parseScoreValueHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        loadedScore = struct.unpack('>d', header[8: 8 + length])[0]
        return ScoreHeader(score=loadedScore)

    def parseAtTXHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        atTx = int.from_bytes(header[8:8 + length], byteorder='big')
        return AtTXHeader(seenAtTx=atTx)

    def chunks(self):
        for chunk in self.streamToRead:
            yield self.reader(chunk)

    def headerReader(self, chunk):
        self.reader = self.scoreValueHeaderReader
        return self.parseHeader(chunk.content)

    def setHeaderReader(self, chunk):
        self.reader = self.headerReader
        return self.parseSetHeader(chunk.content)

    def scoreValueHeaderReader(self, chunk):
        self.reader = self.atTXHeaderReader
        readed = self.parseScoreValueHeader(chunk.content)
        return readed

    def atTXHeaderReader(self, chunk):
        self.reader = self.valueHeaderReader
        readed = self.parseAtTXHeader(chunk.content)
        return readed

    def valueHeaderReader(self, chunk):
        self.reader = self.valueReader
        readed = self.parseValueHeader(chunk.content)
        if (self.left == 0):
            self.reader = self.setHeaderReader
        return readed

    def valueReader(self, chunk):
        self.left = self.left - len(chunk.content)
        readed = ValueChunk(chunk=chunk.content, left=self.left)
        if (self.left == 0):
            self.reader = self.setHeaderReader
        return readed


class BufferedStreamReader:
    def __init__(self, chunksGenerator, valueHeader: ValueChunk, stream):
        self.chunksGenerator = chunksGenerator
        self.size = valueHeader.left + len(valueHeader.chunk)
        self.currentChunk = valueHeader.chunk
        self.readed = 0
        self.currentChunkOffset = 0
        self.currentChunkLength = len(self.currentChunk)
        self.stream = stream

    def __len__(self):
        return self.size

    def _read_new_chunk(self):
        nextChunk = next(self.chunksGenerator, None)
        if (not nextChunk):
            self.currentChunk = None
            return
        self.currentChunk = nextChunk.chunk
        self.currentChunkOffset = 0
        self.currentChunkLength = len(self.currentChunk)

    def read(self, length: int = None) -> bytes:
        if length == None:
            length = self.size
        if (self.readed >= self.size):
            return None
        if (self.readed + length >= self.size):
            length = self.size - self.readed
        bytesToReturn = self.currentChunk[self.currentChunkOffset: self.currentChunkOffset + length]
        self.currentChunkOffset = self.currentChunkOffset + length
        while (len(bytesToReturn) < length):
            self._read_new_chunk()
            if self.currentChunk == None:
                self.readed = self.readed + len(bytesToReturn)
                return bytesToReturn
            self.currentChunkOffset = length - len(bytesToReturn)
            bytesToReturn = bytesToReturn + \
                self.currentChunk[0:self.currentChunkOffset]

        self.readed = self.readed + length
        return bytesToReturn

    def close(self):
        self.stream.cancel()
