from immudb import grpc
import immudb.datatypesv2 as datatypesv2
from immudb.grpc.schema_pb2 import Key, ExecAllRequest
from immudb.dataconverter import convertResponse
import immudb.grpc.schema_pb2 as schema

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

    assert datatypesv2.NullableBool(None)._getGRPC() == None
    assert datatypesv2.NullableBool(True)._getGRPC() == schema.NullableBool(value = True)

    assert datatypesv2.NullableFloat(None)._getGRPC() == None
    assert datatypesv2.NullableFloat(0.123)._getGRPC() == schema.NullableFloat(value = 0.123)

    assert datatypesv2.NullableMilliseconds(None)._getGRPC() == None
    assert datatypesv2.NullableMilliseconds(123123)._getGRPC() == schema.NullableMilliseconds(value = 123123)

    assert datatypesv2.NullableString(None)._getGRPC() == None
    assert datatypesv2.NullableString("")._getGRPC() == schema.NullableString(value = "")

    assert datatypesv2.NullableUint32(None)._getGRPC() == None
    assert datatypesv2.NullableUint32(0)._getGRPC() == schema.NullableUint32(value = 0)

    assert datatypesv2.NullableUint64(None)._getGRPC() == None
    assert datatypesv2.NullableUint64(0)._getGRPC() == schema.NullableUint64(value = 0)



