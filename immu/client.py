import grpc
from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu.handler import safeGet, safeSet

class ImmuClient:
    def __init__(self, immudUrl):
        self.__channel = grpc.insecure_channel(immudUrl)
        self.__stub = schema_pb2_grpc.ImmuServiceStub(self.__channel)
        
        rs = RootService(self.__stub)
        rs.init()

        self.__rs = rs
    
    @property
    def stub(self):
        return self.__stub

    def shutdown(self):
        self.__channel.close()

    def safeGet(self, request:  schema_pb2.SafeGetOptions):
        return safeGet.call(self.__stub, self.__rs, request)

    def safeSet(self, request:  schema_pb2.SafeSetOptions):
        return safeSet.call(self.__stub, self.__rs, request)
