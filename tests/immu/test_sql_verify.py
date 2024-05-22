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

import time
from immudb import datatypesv2
from immudb.handler.sqldescribe import ColumnDescription
from immudb.typeconv import py_to_sqlvalue, sqlvalue_to_py

from datetime import datetime, timedelta, timezone
import pytz
from tests.immuTestClient import ImmuTestClient
import pytest


class TestVerifySQL:

    def test_exec_query(self, wrappedClient: ImmuTestClient):
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            return
        tabname = wrappedClient.createTestTable(
            "id INTEGER", "name VARCHAR", "test INTEGER NOT NULL",  "PRIMARY KEY (id, test)")
        wrappedClient.insertToTable(tabname, ["id", "name", "test"], [
                                    "@id", "@name", "@test"], {'id': 1, 'name': 'Joe', "test": 3})
        wrappedClient.insertToTable(tabname, ["id", "name", "test"], [
                                    "@id", "@name", "@test"], {'id': 2, 'name': 'Joe', "test": 2})
        wrappedClient.insertToTable(tabname, ["id", "name", "test"], [
                                    "@id", "@name", "@test"], {'id': 33, 'name': 'Joe', "test": 111})
        wrappedClient.insertToTable(tabname, ["id", "name", "test"], [
                                    "@id", "@name", "@test"], {'id': 3, 'name': 'Joe', "test": 1})
        result = wrappedClient.simpleSelect(
            tabname, ["id", "name"], {'id': 1}, "id=@id")
        assert (len(result) > 0)
        assert (result == [(1, "Joe")])

        ww = wrappedClient.client.verifiableSQLGet(
            tabname, [datatypesv2.PrimaryKeyIntValue(
                1), datatypesv2.PrimaryKeyIntValue(3)]
        )
        assert ww.verified == True

    def test_varchar(self, wrappedClient: ImmuTestClient):
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            return
        tabname = wrappedClient.createTestTable(
            "name VARCHAR[128]", "test INTEGER NOT NULL",  "PRIMARY KEY (name)")
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 1, 'name': 'Joe', "test": 3})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 2, 'name': 'Joe1', "test": 2})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 33, 'name': 'Joe2', "test": 111})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 3, 'name': 'Joe3', "test": 1})
        result = wrappedClient.simpleSelect(
            tabname, ["name"], {'name': "Joe"}, "name=@name")
        assert (len(result) > 0)
        assert (result == [("Joe",)])

        ww = wrappedClient.client.verifiableSQLGet(
            tabname, [datatypesv2.PrimaryKeyVarCharValue("Joe")]
        )
        assert ww.verified == True

    def test_boolean(self, wrappedClient: ImmuTestClient):
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            return
        tabname = wrappedClient.createTestTable(
            "name VARCHAR[128]", "test BOOLEAN NOT NULL",  "PRIMARY KEY (name, test)")
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 1, 'name': 'Joe', "test": True})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 2, 'name': 'Joe1', "test": False})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 33, 'name': 'Joe2', "test": True})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 3, 'name': 'Joe3', "test": False})
        result = wrappedClient.simpleSelect(
            tabname, ["name"], {'name': "Joe"}, "name=@name")
        assert (len(result) > 0)
        assert (result == [("Joe",)])

        ww = wrappedClient.client.verifiableSQLGet(
            tabname, [datatypesv2.PrimaryKeyVarCharValue(
                "Joe"), datatypesv2.PrimaryKeyBoolValue(True)]
        )
        assert ww.verified == True

    def test_blob(self, wrappedClient: ImmuTestClient):
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            return
        tabname = wrappedClient.createTestTable(
            "name BLOB[128]", "test BOOLEAN NOT NULL",  "PRIMARY KEY (name, test)")
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 1, 'name': b'Joe', "test": True})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 2, 'name': b'Joe1', "test": False})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 33, 'name': b'Joe2', "test": True})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 3, 'name': b'Joe3', "test": False})
        result = wrappedClient.simpleSelect(
            tabname, ["name"], {'name': b"Joe"}, "name=@name")
        assert (len(result) > 0)
        assert (result == [(b"Joe",)])

        ww = wrappedClient.client.verifiableSQLGet(
            tabname, [datatypesv2.PrimaryKeyBlobValue(
                b"Joe"), datatypesv2.PrimaryKeyBoolValue(True)]
        )
        assert ww.verified == True

    def test_ts(self, wrappedClient: ImmuTestClient):
        if (not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            return
        now = datetime.now().astimezone(timezone.utc)
        tabname = wrappedClient.createTestTable(
            "name TIMESTAMP", "test BOOLEAN NOT NULL",  "PRIMARY KEY (name, test)")
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 1, 'name': now, "test": True})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 2, 'name': now + timedelta(seconds=1), "test": False})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 33, 'name': now + timedelta(seconds=2), "test": True})
        wrappedClient.insertToTable(tabname, ["name", "test"], [
                                    "@name", "@test"], {'id': 3, 'name': now + timedelta(seconds=3), "test": False})
        result = wrappedClient.simpleSelect(
            tabname, ["name"], {'name': now}, "name=@name")
        assert (len(result) > 0)
        assert (result == [(now,)])

        ww = wrappedClient.client.verifiableSQLGet(
            tabname, [datatypesv2.PrimaryKeyTsValue(
                int(now.timestamp()*1e6)), datatypesv2.PrimaryKeyBoolValue(True)]
        )
        assert ww.verified == True
