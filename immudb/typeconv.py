from immudb.grpc import schema_pb2
from pprint import pformat


def py_to_sqlvalue(value):
    sqlValue = None
    if value is None:
        sqlValue = schema_pb2.SQLValue(null=value)
    elif isinstance(value, int):
        sqlValue = schema_pb2.SQLValue(n=value)
    elif isinstance(value, bool):
        sqlValue = schema_pb2.SQLValue(b=value)
    elif isinstance(value, str):
        sqlValue = schema_pb2.SQLValue(s=value)
    elif isinstance(value, bytearray):
        sqlValue = schema_pb2.SQLValue(bs=value)
    else:
        raise TypeError("Type not supported: %s".format(
            value.__class__.__name__))
    return sqlValue


def sqlvalue_to_py(sqlValue):
    if sqlValue.HasField("n"):
        return sqlValue.n
    elif sqlValue.HasField("b"):
        return sqlValue.b
    elif sqlValue.HasField("bs"):
        return sqlValue.bs
    elif sqlValue.HasField("s"):
        return sqlValue.s
    elif sqlValue.HasField("null"):
        return None
    else:
        raise TypeError("Type not supported: %s", sqlValue.WhichOneof("value"))
