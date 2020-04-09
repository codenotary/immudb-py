import struct
from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item

@dataclass
class SafeGetResponse:
    index: int
    key: bytes
    value: bytes
    timestamp: int
    verified: bool

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.SafeGetOptions):
    root = rs.get()

    index = schema_pb2.Index(index = root.index)

    rawRequest = schema_pb2.SafeGetOptions(
        key = request.key,
        rootIndex = index
    )

    msg = service.SafeGet(rawRequest)
    verified = proofs.verify(msg.proof, item.digest(msg.item.index, msg.item.key, msg.item.value), root)

    if verified:
        toCache = schema_pb2.Root(
            index = msg.proof.at,
            root = msg.proof.root
        )

        try:
            rs.set(toCache)
        except:
            raise

    i = msg.item

    return SafeGetResponse(
        index = i.index,
        key = i.key,
        value = i.value[8:].decode("utf-8"),
        timestamp = struct.unpack(">Q", i.value[:8])[0],
        verified = verified
    )
