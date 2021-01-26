from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key:bytes, prefix:bytes, desc:bool, limit:int, sinceTx:int):
    if sinceTx==None:
        state = rs.get()
        sinceTx=state.txId
    request = schema_pb2_grpc.schema__pb2.ScanRequest(
        seekKey=key,
        prefix=prefix,
        desc=desc,
        limit=limit,
        sinceTx=sinceTx,
        noWait=False
        )
    msg = service.Scan(request)
    ret={}
    for i in msg.entries:
        ret[i.key]=i.value
    return ret
