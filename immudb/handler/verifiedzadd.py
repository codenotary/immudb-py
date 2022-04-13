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

from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb.exceptions import ErrCorruptedData
from immudb import datatypes
from immudb.embedded import store
import immudb.database as database
import immudb.schema as schema


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, zset: bytes, score: float, key: bytes, atTx: int = 0, verifying_key=None):
    state = rs.get()
    request = schema_pb2.VerifiableZAddRequest(
        zAddRequest=schema_pb2.ZAddRequest(
            set=zset,
            score=score,
            key=key,
            atTx=atTx,
        ),
        proveSinceTx=state.txId
    )
    vtx = service.VerifiableZAdd(request)
    if vtx.tx.header.nentries != 1:
        raise ErrCorruptedData
    tx = schema.TxFromProto(vtx.tx)
    entrySpecDigest = store.EntrySpecDigestFor(tx.header.version)
    ekv = database.EncodeZAdd(zset, score, key, atTx)
    inclusionProof = tx.Proof(ekv.key)
    verifies = store.VerifyInclusion(
        inclusionProof, entrySpecDigest(ekv), tx.header.eh)
    if not verifies:
        raise ErrCorruptedData
    if tx.header.eh != schema.DigestFromProto(vtx.dualProof.targetTxHeader.eH):
        raise ErrCorruptedData

    sourceID = state.txId
    sourceAlh = schema.DigestFromProto(state.txHash)
    targetID = tx.header.iD
    targetAlh = tx.header.Alh()
    if state.txId > 0:
        verifies = store.VerifyDualProof(
            schema.DualProofFromProto(vtx.dualProof),
            sourceID,
            targetID,
            sourceAlh,
            targetAlh,
        )
        if not verifies:
            raise ErrCorruptedData

    newstate = State(
        db=state.db,
        txId=targetID,
        txHash=targetAlh,
        publicKey=vtx.signature.publicKey,
        signature=vtx.signature.signature,
    )
    if verifying_key != None:
        newstate.Verify(verifying_key)
    rs.set(newstate)
    return datatypes.SetResponse(
        id=vtx.tx.header.id,
        verified=True,
    )
