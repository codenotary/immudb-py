from immudb import grpc
import immudb.datatypesv2 as datatypesv2
from immudb.grpc.schema_pb2 import Key, ExecAllRequest
from immudb.dataconverter import convertResponse

def test_converting_to_grpc():

    key = datatypesv2.Key(key = b"tet")
    grpcForm = key._getGRPC()
    assert type(grpcForm) == Key
    assert grpcForm.key == b'tet'

    converted = convertResponse(grpcForm)
    assert converted.key == b'tet'

    mustExist = datatypesv2.KeyMustExistPrecondition(key = b'tet')
    precondition = datatypesv2.Precondition(mustExist, None, None)
    op = datatypesv2.Op(datatypesv2.KeyValue(b'test', b'bbb', None))
    ww = datatypesv2.ExecAllRequest([op], False, [precondition])
    assert isinstance(ww._getGRPC(), ExecAllRequest)


