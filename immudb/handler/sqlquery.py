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


def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, query, params, columnNameMode, dbname):
    return _call_with_executor(query, params, columnNameMode, dbname, service.SQLQuery)


def _call_with_executor(query, params, columnNameMode, dbname, executor):
    paramsObj = []
    for key, value in params.items():
        paramsObj.append(schema_pb2.NamedParam(
            name=key, value=py_to_sqlvalue(value)))

    request = schema_pb2.SQLQueryRequest(
        sql=query,
        acceptStream=True,
        params=paramsObj)

    resp = executor(request)
    return RowIterator(resp, columnNameMode, dbname)


def fix_colnames(cols, dbname, columnNameMode):
    if columnNameMode not in [constants.COLUMN_NAME_MODE_DATABASE, constants.COLUMN_NAME_MODE_FULL]:
        return cols

    return [x.replace("[@DB]", dbname.decode("utf-8")) for x in cols]


def unpack_rows(resp, columnNameMode, colNames):
    result = []
    for row in resp.rows:
        if columnNameMode == constants.COLUMN_NAME_MODE_NONE:
            result.append(tuple([sqlvalue_to_py(i) for i in row.values]))
        else:
            result.append(
                dict(zip(colNames, tuple([sqlvalue_to_py(i) for i in row.values]))))
    return result


def getColumnNames(resp, dbname, columnNameMode):
    cols = []
    if columnNameMode == constants.COLUMN_NAME_MODE_NONE:
        return cols

    for column in resp.columns:
        # note that depending on the version parts can be
        # '(dbname.tablename.fieldname)' *or*
        # '(tablename.fieldname)' without dbnname.
        # In that case we mimic the old behavior by using [@DB] as placeholder
        # that will be replaced at higher level.
        parts = column.name.strip("()").split(".")
        if columnNameMode == constants.COLUMN_NAME_MODE_FIELD:
            cols.append(parts[-1])
            continue
        if columnNameMode == constants.COLUMN_NAME_MODE_TABLE:
            cols.append(".".join(parts[-2:]))
            continue
        print(
            "Use of COLUMN_NAME_MODE_DATABASE and COLUMN_NAME_MODE_FULL is deprecated")
        if len(parts) == 2:
            parts.insert(0, "[@DB]")
        if columnNameMode == constants.COLUMN_NAME_MODE_DATABASE:
            cols.append(".".join(parts))
            continue
        if columnNameMode == constants.COLUMN_NAME_MODE_FULL:
            cols.append("("+".".join(parts)+")")
            continue
        raise ErrPySDKInvalidColumnMode
    return fix_colnames(cols, dbname, columnNameMode)


class ClosedIterator(BaseException):
    pass


class RowIterator:
    def __init__(self, grpcIt, colNameMode, dbname) -> None:
        self._grpcIt = grpcIt
        self._nextRow = 0
        self._rows = []
        self._columns = None
        self._colNameMode = colNameMode
        self._dbname = dbname
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        self._fetch_next()

        row = self._rows[self._nextRow]
        self._nextRow = self._nextRow+1
        return row

    def _fetch_next(self):
        if self._closed:
            raise ClosedIterator

        if self._nextRow < len(self._rows):
            return

        res = next(self._grpcIt)
        if self._columns == None:
            self._columns = getColumnNames(res, self._dbname, self._colsMode())

        self._rows = unpack_rows(
            res, self._colNameMode, self._columns)
        self._nextRow = 0

        if len(self._rows) == 0:
            raise StopIteration

    def _colsMode(self):
        return self._colNameMode if self._colNameMode != constants.COLUMN_NAME_MODE_NONE else constants.COLUMN_NAME_MODE_FIELD

    def columns(self):
        self._fetch_next()
        return self._columns

    def close(self):
        if self._closed:
            raise ClosedIterator

        self._grpcIt.cancel()
        self._closed = True
