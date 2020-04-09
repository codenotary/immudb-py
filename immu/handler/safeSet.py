import struct
from time import time
from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item

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

    index = schema_pb2.Index(index = root.index)

    valueBytes = bytearray()
    valueBytes.extend(struct.pack('>Q', int(time())*1000))
    valueBytes.extend(request.kv.value)

    rawRequest = schema_pb2.SafeSetOptions(
        kv = schema_pb2.KeyValue(
            key = request.kv.key, 
            value = bytes(valueBytes)
        ),
        rootIndex = index
    )

    msg = service.SafeSet(rawRequest)

    if bytes(msg.leaf) != item.digest(msg.index, rawRequest.kv.key, rawRequest.kv.value):
        raise Exception("Proof does not match the given item.")

    verified = proofs.verify(msg, bytes(msg.leaf), root)

    if verified:
        toCache = schema_pb2.Root(
            index = msg.index,
            root = msg.root
        )

        try:
            rs.set(toCache)
        except:
            raise

    return SafeSetResponse(
        index = msg.index,
        leaf = msg.leaf,
        root = msg.root,
        at = msg.at,
        inclusionPath = msg.inclusionPath,
        consistencyPath = msg.consistencyPath,
        verified = verified
    )
