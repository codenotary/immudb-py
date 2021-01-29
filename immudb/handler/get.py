from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key:bytes):
    request=schema_pb2.KeyRequest(
        key=key
        )
    try:
        msg = service.Get(request)
    except Exception as e:
        if hasattr(e,'details') and e.details()=='key not found':
            return None
        raise e
    
    return datatypes.GetResponse(
        tx=msg.tx,
        key=msg.key,
        value=msg.value
        )
