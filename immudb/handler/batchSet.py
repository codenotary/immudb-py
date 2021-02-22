from immudb import datatypes
from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService


    
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, kv: dict):
    request=schema_pb2.SetRequest(
        KVs=[schema_pb2.KeyValue(key=key, value=value) for key,value in kv.items()]
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
