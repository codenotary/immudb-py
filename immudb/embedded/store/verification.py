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

from immudb.embedded import store, ahtree
from immudb.constants import *
from immudb.exceptions import ErrUnsupportedTxVersion
import hashlib
import struct


def VerifyInclusion(proof, digest: bytes, root) -> bool:
    if proof == None:
        return False
    leaf = LEAF_PREFIX+digest
    calcRoot = hashlib.sha256(leaf).digest()
    i = proof.leaf
    r = proof.width-1
    for t in proof.terms:
        b = NODE_PREFIX
        if i % 2 == 0 and i != r:
            b = b+calcRoot+t
        else:
            b = b+t+calcRoot
        calcRoot = hashlib.sha256(b).digest()
        i = i//2
        r = r//2
    return i == r and root == calcRoot


def VerifyLinearProof(proof, sourceTxID: int, targetTxID: int, sourceAlh: bytes, targetAlh: bytes) -> bool:
    if proof == None or proof.sourceTxID != sourceTxID or proof.targetTxID != targetTxID:
        return False

    if (proof.sourceTxID == 0 or proof.sourceTxID > proof.targetTxID or
            len(proof.terms) == 0 or sourceAlh != proof.terms[0]):
        return False

    calculatedAlh = proof.terms[0]
    for i in range(1, len(proof.terms)):
        bs = struct.pack(">Q", proof.sourceTxID+i)+calculatedAlh+proof.terms[i]
        calculatedAlh = hashlib.sha256(bs).digest()

    return targetAlh == calculatedAlh


def VerifyDualProof(proof, sourceTxID, targetTxID, sourceAlh, targetAlh):
    if (proof == None or
        proof.sourceTxHeader == None or
        proof.targetTxHeader == None or
        proof.sourceTxHeader.iD != sourceTxID or
            proof.targetTxHeader.iD != targetTxID):
        return False
    if proof.sourceTxHeader.iD == 0 or proof.sourceTxHeader.iD > proof.targetTxHeader.iD:
        return False
    if sourceAlh != proof.sourceTxHeader.Alh():
        return False
    if targetAlh != proof.targetTxHeader.Alh():
        return False
    if sourceTxID < proof.targetTxHeader.blTxID and ahtree.VerifyInclusion(
            proof.inclusionProof,
            sourceTxID,
            proof.targetTxHeader.blTxID,
            leafFor(sourceAlh),
            proof.targetTxHeader.blRoot) == False:
        return False
    if proof.sourceTxHeader.blTxID > 0 and ahtree.VerifyConsistency(
            proof.consistencyProof,
            proof.sourceTxHeader.blTxID,
            proof.targetTxHeader.blTxID,
            proof.sourceTxHeader.blRoot,
            proof.targetTxHeader.blRoot) == False:
        return False
    if proof.targetTxHeader.blTxID > 0 and ahtree.VerifyLastInclusion(
            proof.lastInclusionProof,
            proof.targetTxHeader.blTxID,
            leafFor(proof.targetBlTxAlh),
            proof.targetTxHeader.blRoot) == False:
        return False
    if sourceTxID < proof.targetTxHeader.blTxID:
        ret = VerifyLinearProof(
            proof.linearProof, proof.targetTxHeader.blTxID, targetTxID, proof.targetBlTxAlh, targetAlh)
    else:
        ret = VerifyLinearProof(
            proof.linearProof, sourceTxID, targetTxID, sourceAlh, targetAlh)
    return ret


def EntrySpecDigestFor(version: int):
    if version == 0:
        return EntrySpecDigest_v0
    elif version == 1:
        return EntrySpecDigest_v1
    else:
        raise ErrUnsupportedTxVersion


def EntrySpecDigest_v0(kv: store.EntrySpec) -> bytes:
    md = hashlib.sha256()
    md.update(kv.key)
    valmd = hashlib.sha256()
    valmd.update(kv.value)
    md.update(valmd.digest())
    return md.digest()


def EntrySpecDigest_v1(kv: store.EntrySpec) -> bytes:
    mdbs = b''
    if kv.metadata != None:
        mdbs = kv.metadata.Bytes()
    mdLen = len(mdbs)
    kLen = len(kv.key)
    b = b''
    b = b+mdLen.to_bytes(2, 'big')
    b = b+mdbs
    b = b+kLen.to_bytes(2, 'big')
    b = b+kv.key

    md = hashlib.sha256()
    md.update(b)
    valmd = hashlib.sha256()
    valmd.update(kv.value)
    md.update(valmd.digest())
    return md.digest()


def leafFor(d: bytes) -> bytes:
    b = LEAF_PREFIX+d
    return hashlib.sha256(b).digest()
