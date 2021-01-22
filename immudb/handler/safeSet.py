from time import time
from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import constants

import immudb.store
@dataclass
class SafeSetResponse:
    id: int
    prevAlh: bytes 
    timestamp: int
    eh: bytes
    blTxId: int
    blRoot: bytes
    verified: bool

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key: bytes, value:bytes):
    state = rs.get()
    kv = schema_pb2.KeyValue(key=key, value=value)
    rawRequest = schema_pb2.VerifiableSetRequest(
        setRequest = schema_pb2.SetRequest(KVs=[kv]),
        proveSinceTx= state.txId,
    )
    verifiableTx = service.VerifiableSet(rawRequest)
    tx=immudb.store.TxFrom(verifiableTx.tx)
    inclusionProof=tx.Proof(constants.SET_KEY_PREFIX+key)
    ekv=immudb.store.EncodeKV(key, value)
    verifies=immudb.store.VerifyInclusion(inclusionProof, ekv.Digest(), tx.eh())
    if not verifies:
        raise VerificationException
    if tx.eh() != immudb.store.DigestFrom(verifiableTx.dualProof.targetTxMetadata.eH):
        raise VerificationException
    if state.txId == 0:
        sourceID = tx.iD
        sourceAlh = tx.alh
    else:
        sourceID = state.txId
        sourceAlh = immudb.store.DigestFrom(state.txHash)
    targetID = tx.ID
    targetAlh = tx.Alh

    verifies = immudb.store.VerifyDualProof(
            immudb.htree.DualProofFrom(verifiableTx.dualProof),
            sourceID,
            targetID,
            sourceAlh,
            targetAlh,
    )
    if not verifies:
        raise VerificationException
    return SafeSetResponse(
        id=verifiableTx.tx.metadata.id,
        prevAlh=verifiableTx.tx.metadata.prevAlh,
        timestamp=verifiableTx.tx.metadata.ts,
        eh=verifiableTx.tx.metadata.eH,
        blTxId=verifiableTx.tx.metadata.blTxId,
        blRoot=verifiableTx.tx.metadata.blRoot,
        verified=verifies,
    )
    
