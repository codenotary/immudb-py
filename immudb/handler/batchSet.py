from time import time
from dataclasses import dataclass

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchSetResponse:
    metadata: schema_pb2.TxMetadata

def _packValueTime(kv,tstamp):
    content=schema_pb2.Content(timestamp=tstamp, payload=kv.value)
    kv=schema_pb2.KeyValue(key=kv.key, value=content.SerializeToString())
    return kv
    
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, kv: dict):
    currtime=int(time())
    rawRequest = schema_pb2.ExecAllRequest(
        Op = [ schema_pb2.KeyValue(key=k, value=kv[k]) for k in kv.keys() ]
    )

    md = service.SetBatch(rawRequest)
    return batchSetResponse(
        metadata=md
    )
