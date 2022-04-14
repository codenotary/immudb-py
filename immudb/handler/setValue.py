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

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes
from immudb.embedded.store import KVMetadata
from immudb.typeconv import MetadataToProto


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, key: bytes, value: bytes, metadata: KVMetadata = None):
    schemaMetadata = MetadataToProto(metadata)

    request = schema_pb2.SetRequest(
        KVs=[schema_pb2.KeyValue(key=key, value=value,
                                 metadata=schemaMetadata)]
    )
    msg = service.Set(request)
    return datatypes.SetResponse(
        id=msg.id,
        verified=False,
    )
