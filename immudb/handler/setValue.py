from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key: bytes, value: bytes): 
    request=schema_pb2.SetRequest(
        KVs=[schema_pb2.KeyValue(key=key, value=value)]
        )
    msg = service.Set(request)
    return datatypes.SetResponse(
        id=msg.id,
        prevAlh=msg.prevAlh,
        timestamp=msg.ts,
        eh=msg.eH,
        blTxId=msg.blTxId,
        blRoot=msg.blRoot,
        verified=False,
    )
