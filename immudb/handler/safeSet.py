from time import time
from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import constants, proofs, item, VerificationException

import immudb.store

@dataclass
class SafeSetResponse:
    index: int
    leaf: bytes 
    root: bytes
    at: int
    inclusionPath: bytes
    consistencyPath: bytes
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
    print(verifies)
    # ?? = item.digest(msg.index, rawRequest.kv.key, rawRequest.kv.value)
    #verified = proofs.verify(msg, bytes(msg.leaf), root)
    #if verified:
        #toCache = schema_pb2.RootIndex(
            #index=msg.at,
            #root=msg.root
        #)
        #try:
            #rs.set(toCache)
        #except Exception as e:
            #raise e
    #return SafeSetResponse(
        #index=msg.index,
        #leaf=msg.leaf,
        #root=msg.root,
        #at=msg.at,
        #inclusionPath=msg.inclusionPath,
        #consistencyPath=msg.consistencyPath,
        #verified=verified
    #)
