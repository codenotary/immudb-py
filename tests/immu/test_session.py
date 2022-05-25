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
import time


class TestSessionTransaction:

    def test_unmanaged_session(self, wrappedClient: ImmuTestClient):
        wrappedClient.openSession("immudb", "immudb", b"defaultdb")
        key = wrappedClient.generateKeyName().encode("utf-8")
        a = wrappedClient.get(key)
        assert a == None
        a = wrappedClient.set(key, b'1')
        a = wrappedClient.get(key)
        assert a.value == b'1'
        a = wrappedClient.get(key)
        assert a.value == b'1'
        interface = wrappedClient.newTx()
        print(interface)
        table = wrappedClient.createTestTable("id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
        wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "3"})
        wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "4"})
        wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "5"})
        interface.commit()
        wrappedClient.closeSession()
        wrappedClient.openSession("immudb", "immudb", b"defaultdb")
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
        wrappedClient.closeSession()
        wrappedClient.openSession("immudb", "immudb", b"defaultdb")
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

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            key = wrappedClient.generateKeyName().encode("utf-8")
            a = wrappedClient.get(key)
            assert a == None
            a = wrappedClient.set(key, b'1')
            a = wrappedClient.get(key)
            assert a.value == b'1'
        table = None

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            interface = wrappedClient.newTx()
            table = wrappedClient.createTestTable("id INTEGER AUTO_INCREMENT", "tester VARCHAR[10]", "PRIMARY KEY id")
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "3"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "4"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "5"})
            interface.commit()

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.newTx()
            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5"]

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.currentTx = session.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            interface.rollback()

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.newTx()
            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5"]

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            interface = wrappedClient.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            interface.rollback()
            interface = wrappedClient.newTx()
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "6"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "7"})
            wrappedClient.insertToTable(table, ["tester"], ["@blabla"], {"blabla": "8"})
            interface.commit()
            interface = wrappedClient.newTx()

        with wrappedClient.openManagedSession("immudb", "immudb", b"defaultdb") as session:
            wrappedClient.newTx()
            what = wrappedClient.simpleSelect(table, ["tester"], dict())
            concatenated = [item[0] for item in what]
            assert concatenated == ["3", "4", "5", "6", "7", "8"]

            