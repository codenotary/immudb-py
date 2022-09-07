
from dataclasses import dataclass


@dataclass
class KeyHeader:
    key: bytes
    length: int

@dataclass
class ValueChunk:
    chunk: bytes
    left: int

@dataclass
class FullKeyValue:
    key: bytes
    value: bytes

class StreamReader:
    def __init__(self, stream):
        self.streamToRead = stream
        self.reader = self.headerReader
        self.valueLength = -1
        self.left = -1

    def parseHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        return KeyHeader(length = length, key = header[8:])

    def parseValueHeader(self, header: bytes):
        length = int.from_bytes(header[0:8], byteorder='big')
        self.valueLength = length
        self.left = self.valueLength - len(header[8:])
        return ValueChunk(chunk = header[8:], left = self.left)

    def chunks(self):
        for chunk in self.streamToRead:
            yield self.reader(chunk)

    def headerReader(self, chunk):
        self.reader = self.valueHeaderReader
        return self.parseHeader(chunk.content)

    def valueHeaderReader(self, chunk):
        self.reader = self.valueReader
        return self.parseValueHeader(chunk.content)

    def valueReader(self, chunk):
        self.left = self.left - len(chunk.content)
        return ValueChunk(chunk = chunk.content, left = self.left)

    
