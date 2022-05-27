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
from immudb.typeconv import py_to_sqlvalue
from immudb.typeconv import sqlvalue_to_py
from immudb import constants
from immudb.exceptions import ErrPySDKInvalidColumnMode


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, query, params, columnNameMode):
    return _call_with_executor(query, params, columnNameMode, service.SQLQuery)


def _call_with_executor(query, params, columnNameMode, executor):
    paramsObj = []
    for key, value in params.items():
        paramsObj.append(schema_pb2.NamedParam(
            name=key, value=py_to_sqlvalue(value)))

    request = schema_pb2.SQLQueryRequest(
        sql=query,
        params=paramsObj)

    resp = executor(request)
    result = []

    columnNames = getColumnNames(resp, columnNameMode)

    for row in resp.rows:
        if columnNameMode == constants.COLUMN_NAME_MODE_NONE:
            result.append(tuple([sqlvalue_to_py(i) for i in row.values]))
        else:
            result.append(
                dict(zip(columnNames, tuple([sqlvalue_to_py(i) for i in row.values]))))
    return result


def getColumnNames(resp, columnNameMode):
    columnNames = []
    if columnNameMode:
        for column in resp.columns:
            if columnNameMode == constants.COLUMN_NAME_MODE_FIELD:
                columnNames.append(column.name.strip("()").split(".")[2])
            elif columnNameMode == constants.COLUMN_NAME_MODE_TABLE:
                columnNames.append(column.name.strip("()").split(".", 1)[1])
            elif columnNameMode == constants.COLUMN_NAME_MODE_DATABASE:
                columnNames.append(column.name.strip("()"))
            elif columnNameMode == constants.COLUMN_NAME_MODE_FULL:
                columnNames.append(column.name)
            else:
                raise ErrPySDKInvalidColumnMode
    return columnNames
