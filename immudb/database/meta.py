# Copyright 2022 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from immudb.constants import *
import immudb.embedded.store as store
import struct


def WrapWithPrefix(b: bytes, prefix: bytes) -> bytes:
    return prefix+b


def EncodeKey(key: bytes):
    return WrapWithPrefix(key, SET_KEY_PREFIX)


def EncodeEntrySpec(key: bytes, md: store.KVMetadata, value: bytes):
    es = store.EntrySpec(
        key=WrapWithPrefix(key, SET_KEY_PREFIX),
        md=md,
        value=WrapWithPrefix(value, PLAIN_VALUE_PREFIX)
    )
    return es


def EncodeReference(key: bytes, md: store.KVMetadata, referencedKey: bytes, atTx: int):
    es = store.EntrySpec(key=WrapWithPrefix(key, SET_KEY_PREFIX),
                         md=md,
                         value=WrapReferenceValueAt(
        WrapWithPrefix(referencedKey, SET_KEY_PREFIX), atTx)
    )
    return es


def WrapReferenceValueAt(key: bytes, atTx: int) -> bytes:
    refVal = REFERENCE_VALUE_PREFIX+atTx.to_bytes(8, 'big')+key
    return refVal


def EncodeZAdd(zset: bytes, score: float, key: bytes, attx: int):
    es = store.EntrySpec(key=WrapZAddReferenceAt(zset, score, key, attx),
                         md=None,
                         value=b''
                         )
    return es


def WrapZAddReferenceAt(zset: bytes, score: float, key: bytes, attx: int) -> bytes:
    ekey = SET_KEY_PREFIX+key
    zkey = SORTED_KEY_PREFIX
    zkey += struct.pack(">Q", len(zset))+zset
    zkey += struct.pack(">d", score)
    zkey += struct.pack(">Q", len(ekey))+ekey
    zkey += struct.pack(">Q", attx)
    return zkey
