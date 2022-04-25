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
from datetime import datetime
import pytz

from tests.immuTestClient import ImmuTestClient


class TestSqlTimestamp:

    def test_exec_query_timestamp(self, wrappedClient: ImmuTestClient):
        if(wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            tabname = wrappedClient.createTestTable(
                "id INTEGER", "ts TIMESTAMP", "PRIMARY KEY id")
            wrappedClient.insertToTable(tabname, ["id", "ts"], [
                                        "@id", "NOW()"], {'id': 1})

            tstest = pytz.timezone(
                "US/Eastern").localize(datetime(2022, 5, 6, 1, 2, 3, 123456))

            wrappedClient.insertToTable(tabname, ["id", "ts"], [
                                        "@id", "@ts"], {'id': 2, 'ts': tstest})

            result = wrappedClient.simpleSelect(
                tabname, ["id", "ts"], {'id': 1}, "id=@id")
            assert(len(result) > 0)
            assert(result[0][0] == 1)
            # calculate timediff of now.
            td = (pytz.timezone("UTC").localize(
                datetime.utcnow())-result[0][1]).total_seconds()
            assert(abs(td) < 2)

            result = wrappedClient.simpleSelect(
                tabname, ["id", "ts"], {'id': 2}, "id=@id")

            assert(len(result) > 0)
            assert(result[0] == (2, tstest))
        else:
            pytest.skip()
            #print("Feature wasn't supported before 1.2.0")
