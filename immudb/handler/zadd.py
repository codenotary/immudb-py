from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb.exceptions import VerificationException
from immudb import datatypes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, zset:bytes, score:float, key:bytes, atTx:int=0):
    request=schema_pb2.ZAddRequest(
            set=      zset,
            score=    score,
            key=      key,
            atTx=     atTx,
            boundRef= atTx > 0,
        )
    msg = service.ZAdd(request)
    if msg.nentries!=1:
        raise VerificationException
    return datatypes.SetResponse(
        id=msg.id,
        prevAlh=msg.prevAlh,
        timestamp=msg.ts,
        eh=msg.eH,
        blTxId=msg.blTxId,
        blRoot=msg.blRoot,
        verified=False,
    )
