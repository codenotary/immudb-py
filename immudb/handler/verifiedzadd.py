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

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb.exceptions import VerificationException
from immudb import datatypes
import immudb.store

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, zset:bytes, score:float, key:bytes, atTx:int=0, verifying_key=None):
    state = rs.get()
    request=schema_pb2.VerifiableZAddRequest(
        zAddRequest=schema_pb2.ZAddRequest(
            set=      zset,
            score=    score,
            key=      key,
            atTx=     atTx,
        ),
        proveSinceTx=state.txId
        )
    vtx = service.VerifiableZAdd(request)
    if vtx.tx.metadata.nentries!=1:
        raise VerificationException
    tx = immudb.store.TxFrom(vtx.tx)
    ekv = immudb.store.EncodeZAdd(zset, score, key, atTx)
    inclusionProof=tx.Proof(ekv.key)
    verifies = immudb.store.VerifyInclusion(inclusionProof, ekv.Digest(), tx.eh())
    if not verifies:
        raise VerificationException
    if tx.eh() != immudb.store.DigestFrom(vtx.dualProof.targetTxMetadata.eH):
        raise VerificationException
    if state.txId == 0:
        sourceID = tx.ID
        sourceAlh = tx.Alh
    else:
        sourceID = state.txId
        sourceAlh = immudb.store.DigestFrom(state.txHash)
    targetID = tx.ID
    targetAlh = tx.Alh
    verifies = immudb.store.VerifyDualProof(
            immudb.htree.DualProofFrom(vtx.dualProof),
            sourceID,
            targetID,
            sourceAlh,
            targetAlh,
    )
    if not verifies:
        raise VerificationException

    newstate=State(
            db       = state.db,
            txId     = targetID,
            txHash   = targetAlh,
            publicKey= vtx.signature.publicKey,
            signature= vtx.signature.signature,
            )
    if verifying_key!=None:
        newstate.Verify(verifying_key)
    rs.set(newstate)
    return datatypes.SetResponse(
        id=vtx.tx.metadata.id,
        verified=True,
    )
