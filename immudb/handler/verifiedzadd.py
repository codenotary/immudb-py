from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb.exceptions import VerificationException
from immudb import datatypes
import immudb.store

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, zset:bytes, score:float, key:bytes, atTx:int=0):
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

    state=schema_pb2.ImmutableState(
            txId=      targetID,
            txHash=    targetAlh,
            signature= vtx.signature,
            )
    rs.set(state)
    return datatypes.SetResponse(
        id=vtx.tx.metadata.id,
        prevAlh=vtx.tx.metadata.prevAlh,
        timestamp=vtx.tx.metadata.ts,
        eh=vtx.tx.metadata.eH,
        blTxId=vtx.tx.metadata.blTxId,
        blRoot=vtx.tx.metadata.blRoot,
        verified=True,
    )
