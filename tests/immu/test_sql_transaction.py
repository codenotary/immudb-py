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

from tests.immuTestClient import ImmuTestClient
class TestSQLTransaction:
    def test_sql_simple_transaction_multiline(self, wrappedClient: ImmuTestClient):
        # BEFORE 1.2.0 we can't pass ; at the end of BEGIN TRANSACTION
        countQuery = "COUNT(*)"
        if(not wrappedClient.amIHigherOrEqualsToVersion("1.2.0")):
            wrappedClient.transactionStart = "BEGIN TRANSACTION"
            countQuery = "COUNT()"

        tabname = wrappedClient.createTestTable("id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
        params = dict()
        queries = []
        for index in range(0, 10):
            paramName = "tester{index}".format(index = index)
            params[paramName] = str(index)
            queries.append(wrappedClient.prepareInsertQuery(tabname, ["tester"], ["@" + paramName]))

        wrappedClient.executeWithTransaction(params, queries)

        counted = wrappedClient.simpleSelect(tabname, [countQuery], dict())
        assert(len(counted) > 0)
        assert(counted[0][0] == 10)
    
    def test_sql_simple_transaction_flat(self, wrappedClient: ImmuTestClient):
        # BEFORE 1.2.0 we can't pass ; at the end of BEGIN TRANSACTION
        countQuery = "COUNT(*)"
        if(not wrappedClient.amIHigherOrEqualsToVersion("1.2.0")):
            wrappedClient.transactionStart = "BEGIN TRANSACTION"
            countQuery = "COUNT()"

        tabname = wrappedClient.createTestTable("id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
        params = dict()
        queries = []
        for index in range(0, 10):
            paramName = "tester{index}".format(index = index)
            params[paramName] = str(index)
            queries.append(wrappedClient.prepareInsertQuery(tabname, ["tester"], ["@" + paramName]))

        wrappedClient.executeWithTransaction(params, queries, separator = " ")

        counted = wrappedClient.simpleSelect(tabname, [countQuery], dict())
        assert(len(counted) > 0)
        assert(counted[0][0] == 10)


