from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb.exceptions import VerificationException

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, 
            zset:bytes, seekKey:bytes, seekScore:float,
            seekAtTx:int, inclusive: bool, limit:int, desc:bool, minscore:float,
            maxscore:float, sinceTx, nowait):
    request=schema_pb2.ZScanRequest(
        set=zset,
        seekKey = seekKey,
        seekScore = seekScore,
        seekAtTx = seekAtTx,
        inclusiveSeek = inclusive,
        limit = limit,
        desc = desc,
        minScore = schema_pb2.Score(score=minscore),
        maxScore = schema_pb2.Score(score=maxscore),
        sinceTx = sinceTx,
        noWait = nowait,
        )
    msg = service.ZScan(request)
    return msg
