# Copyright 2022 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from immudb import constants
from tests.immuTestClient import ImmuTestClient


class Test:
    def test_sql_diffent_columns_response(self, wrappedClient: ImmuTestClient):
        # BEFORE 1.2.0 we can't pass ; at the end of BEGIN TRANSACTION
        queryToCount = "COUNT(*)"
        if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            wrappedClient.transactionStart = "BEGIN TRANSACTION"
            queryToCount = "COUNT()"

        tabname = wrappedClient.createTestTable(
            "id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
        params = dict()
        queries = []
        for index in range(0, 10):
            paramName = "tester{index}".format(index=index)
            params[paramName] = str(index)
            queries.append(wrappedClient.prepareInsertQuery(
                tabname, ["tester"], ["@" + paramName]))

        wrappedClient.executeWithTransaction(params, queries)

        # COLUMN_NAME_MODE_TABLE - TABLE + . + FIELD
        returned = wrappedClient.simpleSelect(tabname, ["tester", "id"], dict(), columnNameMode=constants.COLUMN_NAME_MODE_TABLE)
        for fieldName in returned:
            assert tabname + ".tester" in fieldName
            assert tabname + ".id" in fieldName

        # COLUMN_NAME_MODE_DATABASE - DATABASE + . + TABLE + . + FIELD
        returned = wrappedClient.simpleSelect(tabname, ["tester", "id"], dict(), columnNameMode=constants.COLUMN_NAME_MODE_DATABASE)
        for fieldName in returned:
            assert "defaultdb." + tabname + ".tester" in fieldName
            assert "defaultdb." + tabname + ".id" in fieldName

        # COLUMN_NAME_MODE_FIELD - FIELD
        returned = wrappedClient.simpleSelect(tabname, ["tester", "id"], dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
        for fieldName in returned:
            assert "tester" in fieldName
            assert "id" in fieldName

        # COLUMN_NAME_MODE_FULL - (DATABASE + . + TABLE + . + FIELD)
        returned = wrappedClient.simpleSelect(tabname, ["tester", "id"], dict(), columnNameMode=constants.COLUMN_NAME_MODE_FULL)
        for fieldName in returned:
            assert "(defaultdb." + tabname + ".id" + ")" in fieldName
            assert "(defaultdb." + tabname + ".tester" + ")" in fieldName

        result = wrappedClient.simpleSelect(
            tabname, [queryToCount], {"testvalue": "0"}, "tester=@testvalue", columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
        assert(len(result) == 1)
        for field in result:
            assert "col0" in field
            assert 1 == field["col0"]
