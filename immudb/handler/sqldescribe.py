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
from immudb.datatypes import ColumnDescription
from immudb.typeconv import sqlvalue_to_py
from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, table):
    res = service.DescribeTable(schema_pb2.Table(tableName=table.encode()))
    result = []
    for row in res.rows:
        result.append(
            ColumnDescription(
                name=sqlvalue_to_py(row.values[0]),
                type=sqlvalue_to_py(row.values[1]),
                nullable=sqlvalue_to_py(row.values[2]),
                index=sqlvalue_to_py(row.values[3]),
                autoincrement=sqlvalue_to_py(row.values[4]),
                unique=sqlvalue_to_py(row.values[5]),
            )
        )
    return result
