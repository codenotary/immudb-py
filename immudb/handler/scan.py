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

from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key:bytes, prefix:bytes, desc:bool, limit:int, sinceTx:int):
    if sinceTx==None:
        state = rs.get()
        sinceTx=state.txId
    request = schema_pb2_grpc.schema__pb2.ScanRequest(
        seekKey=key,
        prefix=prefix,
        desc=desc,
        limit=limit,
        sinceTx=sinceTx,
        noWait=False
        )
    msg = service.Scan(request)
    ret={}
    for i in msg.entries:
        ret[i.key]=i.value
    return ret
