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

import pytest
from immudb import constants
from tests.immuTestClient import ImmuTestClient
import time


class TestSessionTransaction:

    def test_simple_unmanaged_session(self, wrappedClient: ImmuTestClient):
        if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            pytest.skip("Version too low")
        txInterface = wrappedClient.client.openSession("immudb", "immudb", b"defaultdb")
        try:
            newTx = txInterface.newTx()
            table = wrappedClient._generateTableName()
            newTx.sqlExec(f"CREATE TABLE {table} (id INTEGER AUTO_INCREMENT, tester VARCHAR[10], PRIMARY KEY id)")
            commit = newTx.commit()
            assert commit.header.id != None

            newTx = txInterface.newTx()
            newTx.sqlExec(f"INSERT INTO {table} (tester) VALUES(@testParam)", params = {"testParam": "123"})
            what = newTx.sqlQuery(f"SELECT * FROM {table}", dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
            assert what == [{"id": 1, "tester": '123'}]
            commit = newTx.commit()
            assert commit.header.id != None

            newTx = txInterface.newTx()
            newTx.sqlExec(f"INSERT INTO {table} (tester) VALUES(@testParam)", params = {"testParam": "321"})
            what = newTx.sqlQuery(f"SELECT * FROM {table}", dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
            assert what == [{"id": 1, "tester": '123'}, {"id": 2, "tester": '321'}]
            commit = newTx.rollback()

            newTx = txInterface.newTx()
            what = newTx.sqlQuery(f"SELECT * FROM {table}", dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
            assert what == [{"id": 1, "tester": '123'}]
            commit = newTx.commit()
            wrappedClient.closeSession()
        finally:
            try:
                wrappedClient.closeSession()
            except:
                pass

    def test_simple_managed_session(self, wrappedClient: ImmuTestClient):
        if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            pytest.skip("Version too low")
        with wrappedClient.client.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            newTx = session.newTx()
            table = wrappedClient._generateTableName()
            newTx.sqlExec(f"CREATE TABLE {table} (id INTEGER AUTO_INCREMENT, tester VARCHAR[10], PRIMARY KEY id)")
            commit = newTx.commit()
            assert commit.header.id != None

            newTx = session.newTx()
            newTx.sqlExec(f"INSERT INTO {table} (tester) VALUES(@testParam)", params = {"testParam": "123"})
            what = newTx.sqlQuery(f"SELECT * FROM {table}", dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
            assert what == [{"id": 1, "tester": '123'}]
            commit = newTx.commit()
            assert commit.header.id != None

        with wrappedClient.client.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            newTx = session.newTx()
            newTx.sqlExec(f"INSERT INTO {table} (tester) VALUES(@testParam)", params = {"testParam": "321"})
            what = newTx.sqlQuery(f"SELECT * FROM {table}", dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
            assert what == [{"id": 1, "tester": '123'}, {"id": 2, "tester": '321'}]
            commit = newTx.rollback()

            newTx = session.newTx()
            what = newTx.sqlQuery(f"SELECT * FROM {table}", dict(), columnNameMode=constants.COLUMN_NAME_MODE_FIELD)
            assert what == [{"id": 1, "tester": '123'}]
            commit = newTx.commit()

    def test_unmanaged_session(self, wrappedClient: ImmuTestClient):
        if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            pytest.skip("Version too low")
        currentTxInterface = wrappedClient.openSession("immudb", "immudb", b"defaultdb")
        try:
            wrappedClient.currentTx = currentTxInterface
            key = wrappedClient.generateKeyName().encode("utf-8")
            a = wrappedClient.get(key)
            assert a == None
            a = wrappedClient.set(key, b'1')
            a = wrappedClient.get(key)
            assert a.value == b'1'
            a = wrappedClient.get(key)
            assert a.value == b'1'
            interface = wrappedClient.newTx()
            table = wrappedClient.createTestTable("id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "3"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "4"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "5"})
            interface.commit()
            wrappedClient.closeSession()
            currentTxInterface = wrappedClient.openSession("immudb", "immudb", b"defaultdb")
            wrappedClient.currentTx = currentTxInterface
            interface = wrappedClient.newTx()

            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5"]

            interface.commit()
            interface = wrappedClient.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            interface.rollback()
        finally:
            try:
                wrappedClient.closeSession()
            except:
                pass
        wrappedClient.currentTx = wrappedClient.openSession("immudb", "immudb", b"defaultdb")
        interface = wrappedClient.newTx()
        what = wrappedClient.simpleSelect(table, ["tester"], dict())
        concatenated = [item[0] for item in what]
        assert concatenated == ["3", "4", "5"]

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            key = wrappedClient.generateKeyName().encode("utf-8")
            a = wrappedClient.get(key)
            assert a == None
            a = wrappedClient.set(key, b'1')
            a = wrappedClient.get(key)
            assert a.value == b'1'

    def test_managed_session(self, wrappedClient: ImmuTestClient):
        if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
            pytest.skip("Version too low")

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            key = wrappedClient.generateKeyName().encode("utf-8")
            a = wrappedClient.get(key)
            assert a == None
            a = wrappedClient.set(key, b'1')
            a = wrappedClient.get(key)
            assert a.value == b'1'
        table = None

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session
            interface = wrappedClient.newTx()
            table = wrappedClient.createTestTable("id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "3"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "4"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "5"})
            interface.commit()

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session
            wrappedClient.newTx()
            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5"]

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session
            interface = wrappedClient.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            interface.rollback()

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session
            wrappedClient.newTx()
            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5"]

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session
            interface = wrappedClient.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            rollbackAs = interface.rollback()
            interface = wrappedClient.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            commitAs = interface.commit()
            assert commitAs.header.id != None
            interface = wrappedClient.newTx()

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session
            wrappedClient.newTx()
            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5", "6", "7", "8"]
            what = wrappedClient.commit()

            