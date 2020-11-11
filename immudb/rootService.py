from immudb import constants
from immudb.grpc import schema_pb2, schema_pb2_grpc
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


class RootService:
    
    def __init__(self, service: schema_pb2_grpc.ImmuServiceStub):
        self.__cache = None
        self.__service = service

    def init(self):
        root = self.__service.CurrentRoot(google_dot_protobuf_dot_empty__pb2.Empty())
        self.__cache=root.payload

    def get(self) -> schema_pb2.RootIndex:
        if self.__cache==None:
            self.init()
        return self.__cache

    def set(self, root: schema_pb2.RootIndex):
        self.__cache=root
