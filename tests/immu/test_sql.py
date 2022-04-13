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

from immudb.handler.sqldescribe import ColumnDescription
from immudb.typeconv import py_to_sqlvalue, sqlvalue_to_py

from datetime import datetime
import pytz
from tests.immuTestClient import ImmuTestClient
import pytest


class TestSql:
    def test_sqlvalue(self):
        for value in (99, None, True, "fives", b'domino',
                  pytz.timezone(
                      "US/Eastern").localize(datetime(2022, 5, 6, 1, 2, 3, 123456)),
                  pytz.timezone("UTC").localize(
                      datetime(2022, 5, 6, 1, 2, 3, 123456))
                  ):
            reverseConvert = sqlvalue_to_py(py_to_sqlvalue(value))
            assert value == reverseConvert
            assert type(value) == type(reverseConvert)
        with pytest.raises(TypeError):
            value = py_to_sqlvalue({'a': 1})

    def test_exec_query(self, wrappedClient: ImmuTestClient):
        tabname = wrappedClient.createTestTable("id INTEGER", "name VARCHAR", "PRIMARY KEY id")
        wrappedClient.insertToTable(tabname, ["id", "name"], ["@id", "@name"], {'id': 1, 'name': 'Joe'})
        result = wrappedClient.simpleSelect(tabname, ["id", "name"], {'id': 1}, "id=@id")
        assert(len(result) > 0)
        assert(result == [(1, "Joe")])

    def test_describe(self, wrappedClient: ImmuTestClient):
        tbname = wrappedClient.createTestTable("id INTEGER", "name VARCHAR[100]", "PRIMARY KEY id")
        
        response = wrappedClient.client.describeTable(tbname)
        assert response == [ColumnDescription(name='id', type='INTEGER', nullable=True, index='PRIMARY KEY', autoincrement=False, unique=True), ColumnDescription(
            name='name', type='VARCHAR[100]', nullable=True, index='NO', autoincrement=False, unique=False)]
