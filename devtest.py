#!/usr/bin/env python
from immu.client import ImmuClient
from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
a = ImmuClient("localhost:3322")
a.login("immudb","immudb")
request2 = schema_pb2_grpc.schema__pb2.SafeSetOptions(kv={"key": b"wololo", "value": b"test"})
request = schema_pb2_grpc.schema__pb2.SafeGetOptions(key=b"wololo")
key = schema_pb2_grpc.schema__pb2.Key(key=b"wololo")
request3 = schema_pb2_grpc.schema__pb2.KeyList(keys=[key])
for _ in range(0, 10):
    a.safeSet(request2)
print(a.safeGet(request))
batch_res = a.getAll(request3)

