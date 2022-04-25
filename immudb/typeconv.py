from immudb.grpc import schema_pb2
from pprint import pformat
from datetime import datetime, timezone
from immudb.embedded.store import KVMetadata


def py_to_sqlvalue(value):
    sqlValue = None
    typ = type(value)
    if value is None:
        sqlValue = schema_pb2.SQLValue(null=True)
    elif typ is int:
        sqlValue = schema_pb2.SQLValue(n=value)
    elif typ is bool:
        sqlValue = schema_pb2.SQLValue(b=value)
    elif typ is str:
        sqlValue = schema_pb2.SQLValue(s=value)
    elif typ in (bytes, bytearray):
        sqlValue = schema_pb2.SQLValue(bs=value)
    elif typ is datetime:
        sqlValue = schema_pb2.SQLValue(
            ts=int(value.timestamp()*1e6))
    else:
        raise TypeError("Type not supported: {}".format(
            value.__class__.__name__))
    return sqlValue


def sqlvalue_to_py(sqlValue):
    if sqlValue.HasField("n"):
        return sqlValue.n
    elif sqlValue.HasField("b"):
        return bool(sqlValue.b)
    elif sqlValue.HasField("bs"):
        return sqlValue.bs
    elif sqlValue.HasField("s"):
        return sqlValue.s
    elif sqlValue.HasField("ts"):
        return datetime.fromtimestamp(sqlValue.ts/1e6, timezone.utc)
    elif sqlValue.HasField("null"):
        return None
    else:
        raise TypeError("Type not supported: {}".format(
            sqlValue.WhichOneof("value")))


def MetadataToProto(metadata: KVMetadata):
    schemaMetadata = None
    if metadata:
        schemaMetadata = schema_pb2.KVMetadata()
        if metadata.Deleted():
            schemaMetadata.deleted = True
        if metadata.IsExpirable():
            schemaMetadata.expiration.expiresAt = int(
                metadata.ExpirationTime().timestamp())
        if metadata.NonIndexable():
            schemaMetadata.nonIndexable = True
    return schemaMetadata
