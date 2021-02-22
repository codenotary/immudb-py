from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, tx:int):
    request=schema_pb2.TxRequest(
        tx=tx
        )
    try:
        msg = service.TxById(request)
    except Exception as e:
        if hasattr(e,'details') and e.details()=='tx not found':
            return None
        raise e
    ret=[]
    for t in msg.entries:
        ret.append(t.key[1:])
    return ret
