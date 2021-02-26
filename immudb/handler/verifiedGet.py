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

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb import constants, htree, store, datatypes
from immudb.exceptions import VerificationException

import sys
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, requestkey: bytes, atTx:int=None, verifying_key=None):
    state = rs.get()
    if atTx==None:
        req = schema_pb2.VerifiableGetRequest(
            keyRequest= schema_pb2.KeyRequest(key=requestkey),
            proveSinceTx= state.txId
            )
    else:
        req = schema_pb2.VerifiableGetRequest(
            keyRequest= schema_pb2.KeyRequest(key=requestkey, atTx=atTx),
            proveSinceTx= state.txId
            )
    ventry=service.VerifiableGet(req)
    inclusionProof = htree.InclusionProofFrom(ventry.inclusionProof)
    dualProof = htree.DualProofFrom(ventry.verifiableTx.dualProof)
    
    if ventry.entry.referencedBy==None or ventry.entry.referencedBy.key==b'':
        vTx=ventry.entry.tx
        kv=store.EncodeKV(requestkey, ventry.entry.value)
    else:
        vTx = ventry.entry.referencedBy.tx
        kv=store.EncodeReference(ventry.entry.referencedBy.key, ventry.entry.key, ventry.entry.referencedBy.atTx) 
        
    if state.txId <= vTx:
        eh=store.DigestFrom(ventry.verifiableTx.dualProof.targetTxMetadata.eH)
        sourceid=state.txId
        sourcealh=store.DigestFrom(state.txHash)
        targetid=vTx
        targetalh=dualProof.targetTxMetadata.alh()
    else:
        eh=store.DigestFrom(ventry.verifiableTx.dualProof.sourceTxMetadata.eH)
        sourceid=vTx
        sourcealh=dualProof.sourceTxMetadata.alh()
        targetid=state.txId
        targetalh=store.DigestFrom(state.txHash)
        
    verifies = store.VerifyInclusion(inclusionProof,kv.Digest(),eh)
    if not verifies:
        raise VerificationException
    verifies=store.VerifyDualProof(
        dualProof,
        sourceid,
        targetid,
        sourcealh,
        targetalh)
    if not verifies:
        raise VerificationException
    newstate=State(
            db       = state.db,
            txId     = targetid,
            txHash   = targetalh,
            publicKey= ventry.verifiableTx.signature.publicKey,
            signature= ventry.verifiableTx.signature.signature,
            )
    if verifying_key!=None:
        newstate.Verify(verifying_key)
    rs.set(newstate)
    if ventry.entry.referencedBy!=None and ventry.entry.referencedBy.key!=b'':
        refkey=ventry.entry.referencedBy.key
    else:
        refkey=None
        
    return datatypes.SafeGetResponse(
        id=vTx,
        key=ventry.entry.key,
        value=ventry.entry.value,
        timestamp=ventry.verifiableTx.tx.metadata.ts,
        verified=verifies,
        refkey=refkey
        )
