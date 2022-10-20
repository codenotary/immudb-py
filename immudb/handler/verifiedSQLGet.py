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
from immudb.exceptions import ErrCorruptedData
import immudb.schema as schema
from typing import List
from immudb import datatypesv2
from immudb.dataconverter import convertResponse


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, table: str, primaryKeys: List[datatypesv2.PrimaryKey], atTx: int, sinceTx: int, verifying_key=None):
    state = rs.get()
    pkValues = [pk._getGRPC() for pk in primaryKeys]
    req = schema_pb2.VerifiableSQLGetRequest(
        sqlGetRequest=schema_pb2.SQLGetRequest(
            table=table,
            pkValues=pkValues,
            atTx=atTx,
            sinceTx=sinceTx
        ),
        proveSinceTx=state.txId
    )
    ventry = service.VerifiableSQLGet(req)
    entrySpecDigest = store.EntrySpecDigestFor(
        int(ventry.verifiableTx.tx.header.version))
    inclusionProof = schema.InclusionProofFromProto(ventry.inclusionProof)
    dualProof = schema.DualProofFromProto(ventry.verifiableTx.dualProof)
    if len(ventry.ColNamesById) == 0:
        raise ErrCorruptedData

    dbID = ventry.DatabaseId
    tableID = ventry.TableId
    valbuf = b''
    for index in range(len(primaryKeys)):
        pkCurrent = primaryKeys[index]
        pkID = ventry.PKIDs[index]
        pkType = ventry.ColTypesById[pkID]
        pkLen = ventry.ColLenById[pkID]
        pkEncval = store.encodeAsKey(pkCurrent.value, pkCurrent, int(pkLen))
        valbuf += pkEncval

    pkKey = store.sqlMapKey(b'\x02', 'R.', [
        store.encodeID(dbID), store.encodeID(
            tableID), store.encodeID(0), valbuf
    ])
    vTx = ventry.sqlEntry.tx
    e = store.EntrySpec(key=pkKey, value=ventry.sqlEntry.value, md=None)

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

    simpleList = [x for x in ventry.PKIDs]
    ColNamesById = dict()
    for key in ventry.ColNamesById:
        ColNamesById[key] = ventry.ColNamesById[key]
    ColIdsByName = dict()
    for key in ventry.ColIdsByName:
        ColIdsByName[key] = ventry.ColIdsByName[key]
    ColTypesById = dict()
    for key in ventry.ColTypesById:
        ColTypesById[key] = ventry.ColTypesById[key]
    ColLenById = dict()
    for key in ventry.ColLenById:
        ColLenById[key] = ventry.ColLenById[key]
    return datatypesv2.VerifiableSQLEntry(
        sqlEntry=datatypesv2.SQLEntry(
            tx=ventry.sqlEntry.tx, key=ventry.sqlEntry.key, value=ventry.sqlEntry.value),
        verifiableTx=convertResponse(ventry.verifiableTx),
        inclusionProof=datatypesv2.InclusionProof(
            leaf=inclusionProof.leaf, width=inclusionProof.width, terms=inclusionProof.terms),
        DatabaseId=dbID,
        TableId=tableID,
        PKIDs=simpleList,
        ColNamesById=ColNamesById,
        ColIdsByName=ColIdsByName,
        ColTypesById=ColTypesById,
        ColLenById=ColLenById,
        verified=True
    )
