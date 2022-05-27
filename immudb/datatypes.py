# Copyright 2021 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass
from enum import IntEnum
from immudb.grpc.schema_pb2 import ReadOnly, WriteOnly, ReadWrite


class TxMode(IntEnum):
    ReadOnly = ReadOnly
    WriteOnly = WriteOnly
    ReadWrite = ReadWrite


@dataclass
class SetResponse:
    id: int
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
class historyResponseItem:
    key: bytes
    value: bytes
    tx: int


@dataclass
class GetResponse:
    tx: int
    key: bytes
    value: bytes


@dataclass
class KeyValue():
    key: bytes
    value: bytes


@dataclass
class ZAddRequest():
    set: bytes
    score: float
    key: bytes
    atTx: int
    boundRef: bool
    noWait: bool

    def __init__(self, set, score, key, atTx=0, noWait=False):
        self.set = set
        self.score = score
        self.key = key
        self.atTx = atTx
        self.boundRef = atTx > 0
        self.noWait = noWait


@dataclass
class ReferenceRequest():
    key: bytes
    referencedKey: bytes
    atTx: int
    boundRef: bool
    noWait: bool
    # TODO: Preconditions

    def __init__(self, key, referencedKey, atTx=0, noWait=False):
        self.key = key
        self.referencedKey = referencedKey
        self.atTx = atTx
        self.boundRef = atTx > 0
        self.noWait = noWait


@dataclass
class ColumnDescription:
    name: str
    type: str
    nullable: bool
    index: str
    autoincrement: bool
    unique: bool


@dataclass
class DeleteKeysRequest():
    keys: list
    sinceTx: int
    noWait: bool

    def __init__(self, keys, sinceTx=0, noWait=False):
        self.keys = keys
        self.sinceTx = sinceTx
        self.noWait = noWait
