from time import time
from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchSetResponse:
    index: schema_pb2.Index

def _packValueTime(kv,tstamp):
    content=schema_pb2.Content(timestamp=tstamp, payload=kv.value)
    skv=schema_pb2.StructuredKeyValue(key=kv.key, value=content)
    return skv
    
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KVList):
    currtime=int(time())
    
    rawRequest = schema_pb2.SKVList(
        SKVs = [_packValueTime(kv,currtime) for kv in request.KVs],
    )

    idx = service.SetBatchSV(rawRequest)
    return batchSetResponse(
        index = idx
    )
