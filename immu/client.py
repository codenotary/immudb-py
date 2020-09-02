import grpc
from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu.handler import safeGet, safeSet, batchGet
from immu import header_manipulator_client_interceptor

class ImmuClient:
    def __init__(self, immudUrl):
        if immudUrl is None:
            immudUrl = "localhost:3322"
        self.channel = grpc.insecure_channel(immudUrl)
        self.__stub = schema_pb2_grpc.ImmuServiceStub(self.channel)
        self.withAuthToken = True

    def login(self, username, password):
        #TODO: Maybe separate this
        req = schema_pb2_grpc.schema__pb2.LoginRequest(user=bytes(username, encoding='utf-8'), password=bytes(password, encoding='utf-8'))
        self.__login_response = schema_pb2_grpc.schema__pb2.LoginResponse = self.__stub.Login(req)
        header_interceptor = header_manipulator_client_interceptor.header_adder_interceptor('authorization', self.__login_response.token)
        self.intercept_channel = grpc.intercept_channel(self.channel, header_interceptor)
        self.__stub = schema_pb2_grpc.ImmuServiceStub(self.intercept_channel)
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
    
    def getAll(self, request: schema_pb2.KeyList):
        return batchGet.call(self.__stub, self.__rs, request)