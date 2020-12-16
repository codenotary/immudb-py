from time import time
from dataclasses import dataclass

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchSetResponse:
    index: schema_pb2.Index

def _packValueTime(kv,tstamp):
    content=schema_pb2.Content(timestamp=tstamp, payload=kv.value)
    kv=schema_pb2.KeyValue(key=kv.key, value=content.SerializeToString())
    return kv
    
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KVList):
    currtime=int(time())
    
    rawRequest = schema_pb2.KVList(
        KVs = [_packValueTime(kv,currtime) for kv in request.KVs],
    )

    idx = service.SetBatch(rawRequest)
    return batchSetResponse(
        index = idx
    )
