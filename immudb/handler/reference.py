from dataclasses import dataclass

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, refkey: bytes, key:  bytes):
    request = schema_pb2_grpc.schema__pb2.ReferenceRequest(
        referencedKey = refkey,
        key=key,
        atTx=0,
        boundRef=False
        )
    msg = service.SetReference(request)
    return msg
