from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import constants, item, htree, store, datatypes
from immudb.exceptions import VerificationException


import sys
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, requestkey: bytes, atTx:int=None):
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
    state=schema_pb2.ImmutableState(
            txId=      targetid,
            txHash=    targetalh,
            signature= ventry.verifiableTx.signature,
            )
    rs.set(state)
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
