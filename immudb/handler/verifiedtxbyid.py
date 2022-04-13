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
from immudb import exceptions
from immudb.embedded import store
import immudb.schema as schema


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, tx: int, verifying_key=None):
    state = rs.get()
    request = schema_pb2.VerifiableTxRequest(
        tx=tx,
        proveSinceTx=state.txId
    )
    try:
        vtx = service.VerifiableTxById(request)
    except Exception as e:
        if hasattr(e, 'details') and e.details() == 'tx not found':
            return None
        raise e
    dualProof = schema.DualProofFromProto(vtx.dualProof)
    if state.txId <= vtx.tx.header.id:
        sourceid = state.txId
        sourcealh = schema.DigestFromProto(state.txHash)
        targetid = vtx.tx.header.id
        targetalh = dualProof.targetTxHeader.Alh()
    else:
        sourceid = vtx.tx.header.id
        sourcealh = dualProof.sourceTxHeader.Alh()
        targetid = state.txId
        targetalh = schema.DigestFromProto(state.txHash)
    verifies = store.VerifyDualProof(
        dualProof,
        sourceid,
        targetid,
        sourcealh,
        targetalh)
    if not verifies:
        raise exceptions.ErrCorruptedData
    newstate = State(
        db=state.db,
        txId=targetid,
        txHash=targetalh,
        publicKey=vtx.signature.publicKey,
        signature=vtx.signature.signature,
    )
    if verifying_key != None:
        newstate.Verify(verifying_key)
    rs.set(newstate)

    ret = []
    for t in vtx.tx.entries:
        ret.append(t.key[1:])
    return ret
