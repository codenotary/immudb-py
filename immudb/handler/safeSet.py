from time import time
from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import constants, proofs, item, VerificationException

@dataclass
class SafeSetResponse:
    index: int
    leaf: bytes 
    root: bytes
    at: int
    inclusionPath: bytes
    consistencyPath: bytes
    verified: bool


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.SafeSetOptions):
    root = rs.get()
    index = schema_pb2.Index(index=root.index)
    content = schema_pb2.Content(
        timestamp=int(time()),
        payload=request.kv.value
        )
    kv = schema_pb2.KeyValue(key=request.kv.key, value=content.SerializeToString())
    rawRequest = schema_pb2.SafeSetOptions(kv=kv, rootIndex=index)
    msg = service.SafeSet(rawRequest)
    digest = item.digest(msg.index, rawRequest.kv.key, rawRequest.kv.value)
    verified = proofs.verify(msg, bytes(msg.leaf), root)
    if verified:
        toCache = schema_pb2.RootIndex(
            index=msg.at,
            root=msg.root
        )
        try:
            rs.set(toCache)
        except Exception as e:
            raise e
    return SafeSetResponse(
        index=msg.index,
        leaf=msg.leaf,
        root=msg.root,
        at=msg.at,
        inclusionPath=msg.inclusionPath,
        consistencyPath=msg.consistencyPath,
        verified=verified
    )
