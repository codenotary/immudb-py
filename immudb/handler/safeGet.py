from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import constants, proofs, item, htree, store
from immudb.exceptions import VerificationException

@dataclass
class SafeGetResponse:
    index: int
    key: bytes
    value: bytes
    #timestamp: int
    verified: bool

import sys
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, requestkey: bytes):
    state = rs.get()
    req = schema_pb2.VerifiableGetRequest(
        keyRequest= schema_pb2.KeyRequest(key=requestkey),
        proveSinceTx= state.txId
        )
    ventry=service.VerifiableGet(req)
    inclusionProof = htree.InclusionProofFrom(ventry.inclusionProof)
    if ventry.entry.referencedBy==None or ventry.entry.referencedBy.key==b'':
        vTx=ventry.entry.tx
        kv=store.EncodeKV(requestkey, ventry.entry.value)
    else:
        vTx = ventry.entry.referencedBy.tx
        kv=store.EncodeReference(ventry.entry.referencedBy.key, ventry.entry.key, ventry.entry.referencedBy.atTx) # TODO
    if state.txId <= vTx:
        eh=store.DigestFrom(ventry.verifiableTx.dualProof.targetTxMetadata.eH)
    else:
        eh=store.DigestFrom(ventry.verifiableTx.dualProof.sourceTxMetadata.eH)
    verifies = store.VerifyInclusion(inclusionProof,kv.Digest(),eh)
    if not verifies:
        raise VerificationException
    return SafeGetResponse(
        index=vTx,
        key=ventry.entry.key,
        value=ventry.entry.value,
        verified=verifies,
        )
