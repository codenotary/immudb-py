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

from immudb.exceptions import ErrCorruptedData

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService, State
from immudb import datatypes
from immudb.embedded import store
import immudb.database as database
import immudb.schema as schema
from immudb.typeconv import MetadataToProto

#import base64


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key: bytes, value: bytes, verifying_key=None, metadata=None):
    schemaMetadata = MetadataToProto(metadata)
    state = rs.get()
    # print(base64.b64encode(state.SerializeToString()))
    kv = schema_pb2.KeyValue(key=key, value=value, metadata=schemaMetadata)
    rawRequest = schema_pb2.VerifiableSetRequest(
        setRequest=schema_pb2.SetRequest(KVs=[kv]),
        proveSinceTx=state.txId,
    )
    verifiableTx = service.VerifiableSet(rawRequest)
    # print(base64.b64encode(verifiableTx.SerializeToString()))
    if verifiableTx.tx.header.nentries != 1 or len(verifiableTx.tx.entries) != 1:
        raise ErrCorruptedData
    tx = schema.TxFromProto(verifiableTx.tx)
    entrySpecDigest = store.EntrySpecDigestFor(tx.header.version)
    inclusionProof = tx.Proof(database.EncodeKey(key))
    md = tx.entries[0].metadata()

    if md != None and md.Deleted():
        raise ErrCorruptedData

    e = database.EncodeEntrySpec(key, md, value)

    verifies = store.VerifyInclusion(
        inclusionProof, entrySpecDigest(e), tx.header.eh)
    if not verifies:
        raise ErrCorruptedData
    if tx.header.eh != schema.DigestFromProto(verifiableTx.dualProof.targetTxHeader.eH):
        raise ErrCorruptedData
    sourceID = state.txId
    sourceAlh = schema.DigestFromProto(state.txHash)
    targetID = tx.header.iD
    targetAlh = tx.header.Alh()

    if state.txId > 0:
        verifies = store.VerifyDualProof(
            schema.DualProofFromProto(verifiableTx.dualProof),
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
        publicKey=verifiableTx.signature.publicKey,
        signature=verifiableTx.signature.signature,
    )
    if verifying_key != None:
        newstate.Verify(verifying_key)
    rs.set(newstate)
    return datatypes.SetResponse(
        id=verifiableTx.tx.header.id,
        verified=verifies,
    )
