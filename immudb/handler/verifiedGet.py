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

from immudb.embedded import store
from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb import datatypes
from immudb.exceptions import ErrCorruptedData
import immudb.database as database
import immudb.schema as schema


import sys


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, requestkey: bytes, atTx: int = None, verifying_key=None, sinceTx: int = None):
    state = rs.get()
    req = schema_pb2.VerifiableGetRequest(
        keyRequest=schema_pb2.KeyRequest(
            key=requestkey, atTx=atTx, sinceTx=sinceTx),
        proveSinceTx=state.txId
    )
    ventry = service.VerifiableGet(req)
    entrySpecDigest = store.EntrySpecDigestFor(
        int(ventry.verifiableTx.tx.header.version))
    inclusionProof = schema.InclusionProofFromProto(ventry.inclusionProof)
    dualProof = schema.DualProofFromProto(ventry.verifiableTx.dualProof)

    if not ventry.entry.HasField("referencedBy"):
        vTx = ventry.entry.tx
        e = database.EncodeEntrySpec(requestkey, schema.KVMetadataFromProto(
            ventry.entry.metadata), ventry.entry.value)
    else:
        ref = ventry.entry.referencedBy
        vTx = ref.tx
        e = database.EncodeReference(ref.key, schema.KVMetadataFromProto(
            ref.metadata), ventry.entry.key, ref.atTx)

    if state.txId <= vTx:
        eh = schema.DigestFromProto(
            ventry.verifiableTx.dualProof.targetTxHeader.eH)
        sourceid = state.txId
        sourcealh = schema.DigestFromProto(state.txHash)
        targetid = vTx
        targetalh = dualProof.targetTxHeader.Alh()
    else:
        eh = schema.DigestFromProto(
            ventry.verifiableTx.dualProof.sourceTxHeader.eH)
        sourceid = vTx
        sourcealh = dualProof.sourceTxHeader.Alh()
        targetid = state.txId
        targetalh = schema.DigestFromProto(state.txHash)

    verifies = store.VerifyInclusion(inclusionProof, entrySpecDigest(e), eh)
    if not verifies:
        raise ErrCorruptedData

    if state.txId > 0:
        verifies = store.VerifyDualProof(
            dualProof,
            sourceid,
            targetid,
            sourcealh,
            targetalh)
        if not verifies:
            raise ErrCorruptedData
    newstate = State(
        db=state.db,
        txId=targetid,
        txHash=targetalh,
        publicKey=ventry.verifiableTx.signature.publicKey,
        signature=ventry.verifiableTx.signature.signature,
    )
    if verifying_key != None:
        newstate.Verify(verifying_key)
    rs.set(newstate)
    if ventry.entry.HasField("referencedBy"):
        refkey = ventry.entry.referencedBy.key
    else:
        refkey = None

    return datatypes.SafeGetResponse(
        id=vTx,
        key=ventry.entry.key,
        value=ventry.entry.value,
        timestamp=ventry.verifiableTx.tx.header.ts,
        verified=verifies,
        refkey=refkey
    )
