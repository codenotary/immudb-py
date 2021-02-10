from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes, htree, store, exceptions

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, tx:int):
    state = rs.get()
    request=schema_pb2.VerifiableTxRequest(
        tx=tx,
        proveSinceTx=state.txId
        )
    try:
        vtx = service.VerifiableTxById(request)
    except Exception as e:
        if hasattr(e,'details') and e.details()=='tx not found':
            return None
        raise e
    dualProof = htree.DualProofFrom(vtx.dualProof)
    if state.txId <= vtx.tx.metadata.id:
        sourceid=state.txId
        sourcealh=store.DigestFrom(state.txHash)
        targetid=vtx.tx.metadata.id
        targetalh=dualProof.targetTxMetadata.alh()
    else:
        sourceid=vtx.tx.metadata.id
        sourcealh=dualProof.sourceTxMetadata.alh()
        targetid=state.txId
        targetalh=store.DigestFrom(state.txHash)
    verifies=store.VerifyDualProof(
        dualProof,
        sourceid,
        targetid,
        sourcealh,
        targetalh)
    if not verifies:
        raise exceptions.VerificationException
    state=schema_pb2.ImmutableState(
            txId=      targetid,
            txHash=    targetalh,
            signature= vtx.signature,
            )
    rs.set(state)

    ret=[]
    for t in vtx.tx.entries:
        ret.append(t.key[1:])
    return ret
