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
from immudb.embedded import store, htree
import copy
from immudb.grpc.schema_pb2 import KVMetadata as grpc_KVMetadata
from immudb.grpc.schema_pb2 import TxMetadata as grpc_TxMetadata
from immudb.grpc.schema_pb2 import DualProof as grpc_DualProof
from immudb.grpc.schema_pb2 import TxHeader as grpc_TxHeader
from immudb.grpc.schema_pb2 import LinearProof as grpc_LinearProof
from immudb.grpc.schema_pb2 import InclusionProof as grpc_InclusionProof
from datetime import datetime

import hashlib


def TxFromProto(stx) -> store.Tx():
    header = store.TxHeader()
    header.iD = stx.header.id
    header.ts = stx.header.ts
    header.blTxID = stx.header.blTxId
    header.blRoot = DigestFromProto(stx.header.blRoot)
    header.prevAlh = DigestFromProto(stx.header.prevAlh)

    header.version = int(stx.header.version)
    header.metadata = TxMetadataFromProto(stx.header.metadata)

    header.nentries = int(stx.header.nentries)
    header.eh = DigestFromProto(stx.header.eH)

    entries = []
    for e in stx.entries:
        entries.append(
            store.TxEntry(
                e.key,
                KVMetadataFromProto(e.metadata),
                int(e.vLen),
                DigestFromProto(e.hValue),
                0)
        )

    tx = store.NewTxWithEntries(header, entries)

    tx.BuildHashTree()

    return tx


def KVMetadataFromProto(md: grpc_KVMetadata) -> store.KVMetadata:
    if md == None:
        return None
    kvmd = store.KVMetadata()
    kvmd.AsDeleted(md.deleted)

    if md.HasField("expiration"):
        kvmd.ExpiresAt(datetime.utcfromtimestamp(md.expiration.expiresAt))

    kvmd.AsNonIndexable(md.nonIndexable)

    return kvmd


def InclusionProofFromProto(iproof: grpc_InclusionProof) -> htree.InclusionProof:
    ip = htree.InclusionProof()
    ip.leaf = int(iproof.leaf)
    ip.width = int(iproof.width)
    ip.terms = DigestsFromProto(iproof.terms)
    return ip


def DualProofFromProto(dproof: grpc_DualProof) -> store.DualProof:
    dp = store.DualProof()
    dp.sourceTxHeader = TxHeaderFromProto(dproof.sourceTxHeader)
    dp.targetTxHeader = TxHeaderFromProto(dproof.targetTxHeader)
    dp.inclusionProof = DigestsFromProto(dproof.inclusionProof)
    dp.consistencyProof = DigestsFromProto(dproof.consistencyProof)
    dp.targetBlTxAlh = DigestFromProto(dproof.targetBlTxAlh)
    dp.lastInclusionProof = DigestsFromProto(dproof.lastInclusionProof)
    dp.linearProof = LinearProofFromProto(dproof.linearProof)
    return dp


def TxHeaderFromProto(hdr: grpc_TxHeader) -> store.TxHeader:
    txh = store.TxHeader()
    txh.iD = hdr.id
    txh.prevAlh = DigestFromProto(hdr.prevAlh)
    txh.ts = hdr.ts
    txh.version = int(hdr.version)
    txh.metadata = TxMetadataFromProto(hdr.metadata)
    txh.nentries = int(hdr.nentries)
    txh.eh = DigestFromProto(hdr.eH)
    txh.blTxID = hdr.blTxId
    txh.blRoot = DigestFromProto(hdr.blRoot)
    return txh


def TxMetadataFromProto(md: grpc_TxMetadata) -> store.TxMetadata:
    if md == None:
        return None
    return store.TxMetadata()


def LinearProofFromProto(lproof: grpc_LinearProof) -> store.LinearProof:
    lp = store.LinearProof()
    lp.sourceTxID = lproof.sourceTxId
    lp.targetTxID = lproof.TargetTxId
    lp.terms = DigestsFromProto(lproof.terms)
    return lp


def DigestFromProto(slicedDigest: bytes) -> bytes:
    d = copy.copy(slicedDigest[:hashlib.sha256().digest_size])
    return d


def DigestsFromProto(slicedTerms: list) -> list:
    d = [copy.copy(i) for i in slicedTerms]
    return d
