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

import pytest
from immudb.client import ImmudbClient
from immudb.typeconv import py_to_sqlvalue, sqlvalue_to_py

import grpc._channel
from random import randint


class TestSql:
    def test_sqlvalue(self):
        for v in (99, None, True, "fives", b'domino'):
            vo=sqlvalue_to_py(py_to_sqlvalue(v))
            assert v==vo
            assert type(v)==type(vo)
        try:
            v=py_to_sqlvalue({'a':1})
        except TypeError:
            v="fail"
        assert v=="fail"
            

    def test_exec_query(self, client):
        tabname="testtable{:04d}".format(randint(0, 10000))
        resp = client.sqlExec(
            "create table {table} (id integer, name varchar, primary key id);".format(
                table=tabname 
                )
            )
        assert(len(resp.ctxs) > 0)
        assert(len(resp.dtxs) == 0)

        resp = client.listTables()
        assert(tabname in resp)

        resp = client.sqlExec(
            "insert into {table} (id, name) values (@id, @name);".format(table=tabname),
            {'id': 1, 'name': 'Joe'}
            )
        assert(len(resp.ctxs) == 0)
        assert(len(resp.dtxs) > 0)

        result = client.sqlQuery(
            "select id,name from {table} where id=@id;".format(table=tabname),
            {'id': 1}
            )
        assert(len(result) > 0)

        assert(result == [(1, "Joe")])
