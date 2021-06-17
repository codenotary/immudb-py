# Copyright 2021 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from immudb import datatypes
from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, ops: list, noWait: bool):
    request_ops = []
    for op in ops:
        if type(op) is datatypes.KeyValue:
            request_ops.append(
                schema_pb2.Op(
                    kv=schema_pb2.KeyValue(
                        key=op.key,
                        value=op.value
                    )
                )
            )

        elif type(op) is datatypes.ZAddRequest:
            request_ops.append(
                schema_pb2.Op(
                    zAdd=schema_pb2.ZAddRequest(
                        set=op.set,
                        score=op.score,
                        key=op.key,
                        atTx=op.atTx,
                        boundRef=op.boundRef,
                        noWait=op.noWait
                    )
                )
            )
        elif type(op) is datatypes.ReferenceRequest:
            request_ops.append(
                schema_pb2.Op(
                    ref=schema_pb2.ReferenceRequest(
                        key=op.key,
                        referencedKey=op.referencedKey,
                        atTx=op.atTx,
                        boundRef=op.boundRef,
                        noWait=op.noWait
                    )
                )
            )
        else:
            raise("unknown op for execAll")

    request = schema_pb2.ExecAllRequest(Operations=request_ops, noWait=noWait)
    msg = service.ExecAll(request)
    return msg
