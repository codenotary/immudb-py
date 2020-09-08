from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
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
    msg = service.SafeGetSV(rawRequest)
    verified = proofs.verify(
        msg.proof,
        item.digest(msg.item.index, msg.item.key, msg.item.value.SerializeToString()),
        root
        )
    if verified:
        toCache = schema_pb2.Root(
            index=msg.proof.at,
            root=msg.proof.root
        )
        try:
            rs.set(toCache)
        except:
            raise VerificationException("Failed to verify")
    i = msg.item
    return SafeGetResponse(
        index=i.index,
        key=i.key,
        timestamp=i.value.timestamp,
        value=i.value.payload.decode("utf-8"),
        verified=verified
    )
