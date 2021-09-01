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
import grpc._channel


class TestSql:

    def test_exec_query(self, client):
        resp = client.sqlExec(
            "create table test (id integer, name varchar, primary key id);")
        assert(len(resp.ctxs) > 0)
        assert(len(resp.dtxs) == 0)

        resp = client.listTables()
        assert('test' in resp)

        resp = client.sqlExec(
            "insert into test (id, name) values (@id, @name);", {'id': 1, 'name': 'Joe'})
        assert(len(resp.ctxs) == 0)
        assert(len(resp.dtxs) > 0)

        result = client.sqlQuery(
            "select id,name from test where id=@id;", {'id': 1})
        assert(len(result) > 0)

        assert(result == [(1, "Joe")])
