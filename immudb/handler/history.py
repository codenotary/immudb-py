from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class historyResponse:
    itemlist: schema_pb2.ItemList

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    histo = service.HistorySV(request)
    return historyResponse(
        itemlist=histo
        )
