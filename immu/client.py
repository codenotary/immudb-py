import grpc
from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu.handler import safeGet, safeSet, batchGet, batchSet
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

    def safeGet(self, key: bytes): 
        request=schema_pb2_grpc.schema__pb2.SafeGetOptions(key=key)
        return safeGet.call(self.__stub, self.__rs, request)

    def safeSet(self, key: bytes, value: bytes): 
        request=schema_pb2_grpc.schema__pb2.SafeSetOptions(kv={"key": key, "value": value})
        return safeSet.call(self.__stub, self.__rs, request)
    
    def getAll(self, keys: list):
        klist=[schema_pb2_grpc.schema__pb2.Key(key=k) for k in keys]
        request=schema_pb2_grpc.schema__pb2.KeyList(keys=klist)
        return batchGet.call(self.__stub, self.__rs, request)
    
    def setAll(self, kv_list: list):
        _KVs=[]
        for i in kv_list:
            k=i['key']
            v=i['value']
            _KVs.append(schema_pb2_grpc.schema__pb2.KeyValue(key=k, value=v))
        request=schema_pb2_grpc.schema__pb2.KVList(KVs=_KVs)
        return batchSet.call(self.__stub, self.__rs, request)
