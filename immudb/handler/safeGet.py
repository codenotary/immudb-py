from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import constants, proofs, item
from immudb.VerificationException import VerificationException

@dataclass
class SafeGetResponse:
    index: int
    key: bytes
    value: bytes
    timestamp: int
    verified: bool


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.SafeGetOptions):
    root = rs.get()
    index = schema_pb2.Index(index=root.index)
    rawRequest = schema_pb2.SafeGetOptions(
        key=request.key,
        rootIndex=index
    )
    msg = service.SafeGet(rawRequest)
    verified = proofs.verify(
        msg.proof,
        item.digest(msg.item.index, msg.item.key, msg.item.value),
        root
        )
    if verified:
        toCache = schema_pb2.RootIndex(
            index=msg.proof.at,
            root=msg.proof.root
        )
        try:
            rs.set(toCache)
        except:
            raise VerificationException("Failed to verify")
    content=schema_pb2.Content()
    content.ParseFromString(msg.item.value)
    return SafeGetResponse(
        index=msg.item.index,
        key=msg.item.key,
        timestamp=content.timestamp,
        value=content.payload.decode("utf-8"),
        verified=verified
    )
