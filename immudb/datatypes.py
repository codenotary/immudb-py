from dataclasses import dataclass

@dataclass
class SetResponse:
    id: int
    prevAlh: bytes 
    timestamp: int
    eh: bytes
    blTxId: int
    blRoot: bytes
    verified: bool

@dataclass
class SafeGetResponse:
    id: int
    key: bytes
    value: bytes
    timestamp: int
    verified: bool
    refkey: bytes

@dataclass
class CurrentRootResponse:
    id: int
    hash: bytes

@dataclass
class historyResponseItem:
    key: bytes
    value: bytes
    tx: int

@dataclass
class GetResponse:
    tx: int
    key: bytes
    value: bytes

