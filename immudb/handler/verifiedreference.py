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

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService, State
import immudb.store
from immudb import datatypes
from immudb.exceptions import VerificationException

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, refkey: bytes, key:  bytes, atTx=0, verifying_key=None):
    state = rs.get()
    req = schema_pb2_grpc.schema__pb2.ReferenceRequest(
        referencedKey = refkey,
        key=key,
        atTx=atTx,
        boundRef=atTx>0
        )
    vreq=schema_pb2_grpc.schema__pb2.VerifiableReferenceRequest(
        referenceRequest=req,
        proveSinceTx=state.txId
        )
    vtx = service.VerifiableSetReference(vreq)
    if vtx.tx.metadata.nentries !=1:
        raise VerificationException
    tx = immudb.store.TxFrom(vtx.tx)
    inclusionProof=tx.Proof(immudb.store.EncodeKey(key))
    ekv=immudb.store.EncodeReference(key, refkey, atTx)
    verifies = immudb.store.VerifyInclusion(inclusionProof, ekv.Digest(), tx.eh())
    if not verifies:
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


