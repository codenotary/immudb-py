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
import sys


class TestSQLFloat:

    def test_sql_float_query(self, wrappedClient: ImmuTestClient):
        # Testing the FLOAT type inserting and querying
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.5.0")):
            return

        value_to_test = 1.1

        tabname = wrappedClient.createTestTable(
            "id INTEGER AUTO_INCREMENT", "tester FLOAT", "PRIMARY KEY id"
        )
        wrappedClient.insertToTable(
            tabname, ["tester"], ["@test"], {"test": value_to_test}
        )
        result = wrappedClient.simpleSelect(
            tabname, ["tester"], {"testvalue": value_to_test},
            "tester=@testvalue"
        )

        assert (len(result) > 0)
        assert (result[0][0] == value_to_test)

    def test_sql_float_aggreg_and_filter(self, wrappedClient: ImmuTestClient):
        # Testing the FLOAT type with aggregation and filtering
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.5.0")):
            return

        values_to_test = [1.1, 2.2, 3.3, 4.4]

        tabname = wrappedClient.createTestTable(
            "id INTEGER AUTO_INCREMENT", "tester FLOAT", "PRIMARY KEY id"
        )

        for val in values_to_test:
            wrappedClient.insertToTable(
                tabname, ["tester"], ["@test"], {"test": val}
            )

        result = wrappedClient.simpleSelect(
            tabname,
            ["AVG(tester)"],
            {"min_val": values_to_test[1] + 0.1},
            "tester > @min_val"
        )

        assert (len(result) == 1)
        avg_val = result[0][0]
        expected_avg = sum(values_to_test[2:]) / len(values_to_test[2:])
        assert (abs(avg_val - expected_avg) < sys.float_info.epsilon)
