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


class TestSQLCount:

    def test_sql_count(self, wrappedClient: ImmuTestClient):
        # 1.2.0 introduces COUNT(*) instead of COUNT()
        queryToCount = "COUNT()"
        if wrappedClient.serverHigherOrEqualsToVersion("1.2.0"):
            queryToCount = "COUNT(*)"

        tabname = wrappedClient.createTestTable(
            "id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
        wrappedClient.insertToTable(
            tabname, ["tester"], ["@test"], {"test": "test"})
        result = wrappedClient.simpleSelect(
            tabname, ["tester"], {"testvalue": "test"}, "tester=@testvalue")
        assert(len(result) > 0)
        assert(result[0][0] == "test")

        result = wrappedClient.simpleSelect(
            tabname, [queryToCount], {"testvalue": "test"}, "tester=@testvalue")
        assert(len(result) > 0)
        assert(result[0][0] == 1)

        wrappedClient.insertToTable(
            tabname, ["tester"], ["@test"], {"test": "test"})
        result = wrappedClient.simpleSelect(
            tabname, [queryToCount], {"testvalue": "test"}, "tester=@testvalue")
        assert(len(result) > 0)
        assert(result[0][0] == 2)

        for index in range(0, 10):
            wrappedClient.insertToTable(
                tabname, ["tester"], ["@test"], {"test": "test"})
        result = wrappedClient.simpleSelect(
            tabname, [queryToCount], {"testvalue": "test"}, "tester=@testvalue")
        assert(len(result) > 0)
        assert(result[0][0] == 12)
