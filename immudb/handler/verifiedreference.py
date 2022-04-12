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

from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb.embedded import store
from immudb import datatypes
from immudb.exceptions import ErrCorruptedData

import immudb.database as database
import immudb.schema as schema


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, refkey: bytes, key:  bytes, atTx=0, verifying_key=None):
    state = rs.get()
    req = schema_pb2_grpc.schema__pb2.ReferenceRequest(
        referencedKey=refkey,
        key=key,
        atTx=atTx,
        boundRef=atTx > 0
    )
    vreq = schema_pb2_grpc.schema__pb2.VerifiableReferenceRequest(
        referenceRequest=req,
        proveSinceTx=state.txId
    )
    vtx = service.VerifiableSetReference(vreq)
    if vtx.tx.header.nentries != 1:
        raise ErrCorruptedData
    tx = schema.TxFromProto(vtx.tx)
    entrySpecDigest = store.EntrySpecDigestFor(tx.header.version)
    inclusionProof = tx.Proof(database.EncodeKey(key))

    e = database.EncodeReference(key, None, refkey, atTx)

    verifies = store.VerifyInclusion(
        inclusionProof, entrySpecDigest(e), tx.header.eh)
    if not verifies:
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
