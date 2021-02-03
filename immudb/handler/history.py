from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key: bytes, offset: int, limit: int, desc: bool):
    state = rs.get()
    request = schema_pb2_grpc.schema__pb2.HistoryRequest(
                key=key,
                offset=offset,
                limit=limit,
                desc=desc,
                sinceTx=state.txId
                )
    histo = service.History(request)
    histolist=[]
    for i in histo.entries:
        histolist.append( datatypes.historyResponseItem(
            key=i.key, 
            value=i.value,
            tx=i.tx
            ))
    return histolist
