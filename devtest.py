#!/usr/bin/env python
from immu.client import ImmuClient
from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
a = ImmuClient("localhost:3322")
a.login("immudb","immudb")
request = schema_pb2_grpc.schema__pb2.SafeGetOptions(key=b"a")
print(a.safeGet(request))
