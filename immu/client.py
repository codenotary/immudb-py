import grpc
from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu.handler import safeGet, safeSet

class ImmuClient:
    def __init__(self, immudUrl):
        self.serverUrl = "localhost"
        self.serverPort = "3322"
        self.withAuthToken = True
        self.channel = grpc.insecure_channel(immudUrl)
        self.__stub = schema_pb2_grpc.ImmuServiceStub(self.channel)
        #rs = RootService(self.__stub)
        #rs.init()
        #self.__rs = rs

    def login(self, username, password):
        req = schema_pb2_grpc.schema__pb2.LoginRequest(user=bytes(username, encoding='utf-8'), password=bytes(password, encoding='utf-8'))
        self.__stub.Login(req)

    @property
    def stub(self):
        return self.__stub

    def shutdown(self):
        self.__channel.close()

    def safeGet(self, request:  schema_pb2.SafeGetOptions):
        return safeGet.call(self.__stub, self.__rs, request)

    def safeSet(self, request:  schema_pb2.SafeSetOptions):
        return safeSet.call(self.__stub, self.__rs, request)
