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

from time import time

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb import constants, datatypes, store, htree

#import base64

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key: bytes, value:bytes, verifying_key=None):
    state = rs.get()
    # print(base64.b64encode(state.SerializeToString()))
    kv = schema_pb2.KeyValue(key=key, value=value)
    rawRequest = schema_pb2.VerifiableSetRequest(
        setRequest = schema_pb2.SetRequest(KVs=[kv]),
        proveSinceTx= state.txId,
    )
    verifiableTx = service.VerifiableSet(rawRequest)
    # print(base64.b64encode(verifiableTx.SerializeToString()))
    tx=store.TxFrom(verifiableTx.tx)
    inclusionProof=tx.Proof(constants.SET_KEY_PREFIX+key)
    ekv=store.EncodeKV(key, value)
    verifies=store.VerifyInclusion(inclusionProof, ekv.Digest(), tx.eh())
    if not verifies:
        raise VerificationException
    if tx.eh() != store.DigestFrom(verifiableTx.dualProof.targetTxMetadata.eH):
        raise VerificationException
    if state.txId == 0:
        sourceID = tx.ID
        sourceAlh = tx.Alh
    else:
        sourceID = state.txId
        sourceAlh = store.DigestFrom(state.txHash)
    targetID = tx.ID
    targetAlh = tx.Alh

    verifies = store.VerifyDualProof(
            htree.DualProofFrom(verifiableTx.dualProof),
            sourceID,
            targetID,
            sourceAlh,
            targetAlh,
    )
    if not verifies:
        raise VerificationException
    newstate=State(
            db       = state.db,
            txId=      targetID,
            txHash=    targetAlh,
            publicKey= verifiableTx.signature.publicKey,
            signature= verifiableTx.signature.signature,
            )
    if verifying_key!=None:
        newstate.Verify(verifying_key)
    rs.set(newstate)
    return datatypes.SetResponse(
        id=verifiableTx.tx.metadata.id,
        verified=verifies,
    )
    
