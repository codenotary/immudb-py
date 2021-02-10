from dataclasses import dataclass

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService
import immudb.store
from immudb import datatypes
from immudb.exceptions import VerificationException

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, refkey: bytes, key:  bytes, atTx=0):
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


