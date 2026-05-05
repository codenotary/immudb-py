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

from operator import xor
from typing import List
from immudb import datatypesv2
from immudb.embedded import store, ahtree
from immudb.constants import *
from immudb.exceptions import ErrCorruptedData, ErrUnsupportedTxVersion, ErrMaxKeyLengthExceeded, ErrInvalidValue, ErrMaxLengthExceeded
import hashlib
import struct
import datetime


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


def advanceLinearHash(alh: bytes, txID: int, term: bytes) -> bytes:
    # hash(txID || prevAlh || innerHash) — matches advanceLinearHash in
    # embedded/store/verification.go.
    return hashlib.sha256(struct.pack(">Q", txID) + alh + term).digest()


def VerifyLinearAdvanceProof(proof, startTxID: int, endTxID: int,
                             endAlh: bytes, treeRoot: bytes,
                             treeSize: int) -> bool:
    """Verify the linear chain on the slice [startTxID..endTxID] is consistent
    with `treeRoot` (a Merkle root of size `treeSize`) and ends at `endAlh`.

    Mirrors VerifyLinearAdvanceProof in embedded/store/verification.go: each
    intermediate Alh on the chain must be included in the new Merkle Tree, and
    the final advance must reach `endAlh`. Returns True when no advance proof
    is needed (gap of <=1).
    """
    if endTxID < startTxID:
        return False
    # No advance proof required for adjacent / equal txIDs.
    if endTxID <= startTxID + 1:
        return True
    if (proof is None
            or proof.linearProofTerms is None
            or proof.inclusionProofs is None
            or len(proof.linearProofTerms) != endTxID - startTxID
            or len(proof.inclusionProofs) != endTxID - startTxID - 1):
        return False
    calculatedAlh = proof.linearProofTerms[0]  # alh at startTx+1
    for txID in range(startTxID + 1, endTxID):
        if not ahtree.VerifyInclusion(
                proof.inclusionProofs[txID - startTxID - 1],
                txID,
                treeSize,
                leafFor(calculatedAlh),
                treeRoot):
            return False
        calculatedAlh = advanceLinearHash(
            calculatedAlh, txID + 1, proof.linearProofTerms[txID - startTxID])
    return calculatedAlh == endAlh


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
        if not VerifyLinearProof(
                proof.linearProof, proof.targetTxHeader.blTxID,
                targetTxID, proof.targetBlTxAlh, targetAlh):
            return False
        # The slice from sourceTxID..BlTxID is already covered by InclusionProof
        # above; LinearAdvanceProof seals the linear chain consistency for that
        # gap inside the new Merkle Tree.
        return VerifyLinearAdvanceProof(
            proof.linearAdvanceProof,
            proof.sourceTxHeader.blTxID,
            sourceTxID,
            sourceAlh,
            proof.targetTxHeader.blRoot,
            proof.targetTxHeader.blTxID,
        )
    if not VerifyLinearProof(
            proof.linearProof, sourceTxID, targetTxID, sourceAlh, targetAlh):
        return False
    return VerifyLinearAdvanceProof(
        proof.linearAdvanceProof,
        proof.sourceTxHeader.blTxID,
        proof.targetTxHeader.blTxID,
        proof.targetBlTxAlh,
        proof.targetTxHeader.blRoot,
        proof.targetTxHeader.blTxID,
    )


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


def sqlMapKey(prefix: bytes, mappingPrefix: str, encValues: List[bytes]):

    mkey = b''

    off = 0

    mkey += prefix
    off += len(prefix)

    mkey += mappingPrefix.encode("utf-8")

    off += len(mappingPrefix)

    for ev in encValues:
        mkey += ev
        off += len(ev)

    return mkey


def encodeID(id: int):
    encId = b''
    encId += int.to_bytes(id, 4, "big")
    return encId


def encodeAsKey(val, colType, maxLen):
    maxKeyLen = 256  # pkg/client/sql.go
    KeyValPrefixNotNull = b'\x80'

    if maxLen <= 0:
        raise ErrInvalidValue()

    if maxLen > maxKeyLen:
        raise ErrMaxKeyLengthExceeded()

    if val == None:
        return KeyValPrefixNotNull

    if isinstance(colType, datatypesv2.PrimaryKeyNullValue):
        strVal = str(val)
        if len(strVal) > maxLen:
            raise ErrMaxLengthExceeded()

        encv = b''
        encv[0] = KeyValPrefixNotNull
        encv += strVal.encode("utf-8")
        encv += int.to_bytes(len(strVal), 4, "big")

        return encv
    elif isinstance(colType, datatypesv2.PrimaryKeyIntValue):
        if maxLen != 8:
            raise ErrCorruptedData()

        intVal = int(val)

        encv = bytearray()
        encv += KeyValPrefixNotNull
        encv += int.to_bytes(intVal, 8, "big")
        encv[1] = ord(encv[1:2]) ^ ord(b'\x80')
        return bytes(encv)
    elif isinstance(colType, datatypesv2.PrimaryKeyVarCharValue):

        encv = bytearray()
        encv += KeyValPrefixNotNull
        encv += str(val).encode("utf-8")
        encv += b'\x00' * (maxLen - len(val))
        encv += int.to_bytes(len(val), 4, "big")
        return bytes(encv)
    elif isinstance(colType, datatypesv2.PrimaryKeyBoolValue):

        encv = bytearray()
        encv += KeyValPrefixNotNull
        if (val == True):
            encv += b'\x01'
        else:
            encv += b'\x00'
        return bytes(encv)
    elif isinstance(colType, datatypesv2.PrimaryKeyBlobValue):

        encv = bytearray()
        encv += KeyValPrefixNotNull
        encv += val
        encv += b'\x00' * (maxLen - len(val))
        encv += int.to_bytes(len(val), 4, "big")
        return bytes(encv)
    elif isinstance(colType, datatypesv2.PrimaryKeyTsValue):
        if maxLen != 8:
            raise ErrCorruptedData()

        parsed = datetime.datetime.fromtimestamp(val / 1e6)
        # UnixNano from GO not compatible with python, need to round last int
        intVal = round(int(parsed.timestamp() * 1e9), -3)

        encv = bytearray()
        encv += KeyValPrefixNotNull
        encv += int.to_bytes(intVal, 8, "big")
        encv[1] = ord(encv[1:2]) ^ ord(b'\x80')
        return bytes(encv)
