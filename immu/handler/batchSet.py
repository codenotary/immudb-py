import struct
from time import time
from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item

@dataclass
class batchSetResponse:
    index: schema_pb2.Index

def _packValueTime(kv,tstamp):
    valueBytes = bytearray()
    valueBytes.extend(struct.pack('>Q', tstamp))
    valueBytes.extend(kv.value)
    kv.value=bytes(valueBytes)
    return kv
    
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KVList):
    root = rs.get()
    index = schema_pb2.Index(index = root.index)

    currtime=int(time()*1000)
    
    rawRequest = schema_pb2.KVList(
        KVs = [_packValueTime(kv,currtime) for kv in request.KVs],
    )

    idx = service.SetBatch(rawRequest)
    return batchSetResponse(
        index = idx
    )
