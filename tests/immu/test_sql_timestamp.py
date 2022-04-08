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
from immudb.handler.sqldescribe import ColumnDescription
from immudb.typeconv import py_to_sqlvalue, sqlvalue_to_py

import grpc._channel
from random import randint
from datetime import datetime
import pytz


class TestSqlTimestamp:

    def compare_version(self, a, b):
        aNumber = a.split("-")
        bNumber = b.split("-")
        (aMajor, aMinor, aRev) = aNumber[0].split(".")
        (bMajor, bMinor, bRev) = bNumber[0].split(".")
        if (aMajor, aMinor, aRev) < (bMajor, bMinor, bRev):
            return -1
        elif (aMajor, aMinor, aRev) > (bMajor, bMinor, bRev):
            return 1
        else:
            return 0

    def test_exec_query_timestamp(self, client):

        health = client.health()

        if self.compare_version(health.version, "1.2.0") > -1:

            tabname = "testtable{:04d}".format(randint(0, 10000))
            resp = client.sqlExec(
                "create table {table} (id integer, ts timestamp, primary key id);".format(
                    table=tabname
                )
            )

            # when talking to an older server, we will have unknown fields.
            assert((len(resp.txs) > 0 and not resp.ongoingTx and not resp.UnknownFields())
                   or len(resp.UnknownFields()) > 0)

            resp = client.listTables()
            assert(tabname in resp)

            resp = client.sqlExec(
                "insert into {table} (id, ts) values (@id, NOW());".format(table=tabname),
                {'id': 1}
            )

            assert((len(resp.txs) > 0 and not resp.ongoingTx and not resp.UnknownFields())
                   or len(resp.UnknownFields()) > 0)

            tstest = pytz.timezone(
                "US/Eastern").localize(datetime(2022, 5, 6, 1, 2, 3, 123456))

            resp = client.sqlExec(
                "insert into {table} (id, ts) values (@id, @ts);".format(table=tabname),
                {'id': 2, 'ts': tstest}
            )

            assert((len(resp.txs) > 0 and not resp.ongoingTx and not resp.UnknownFields())
                   or len(resp.UnknownFields()) > 0)

            result = client.sqlQuery(
                "select id, ts from {table} where id=@id;".format(
                    table=tabname),
                {'id': 1}
            )
            assert(len(result) > 0)
            assert(result[0][0] == 1)
            # calculate timediff of now.
            td = (pytz.timezone("UTC").localize(
                datetime.utcnow())-result[0][1]).total_seconds()
            assert(abs(td) < 2)

            result = client.sqlQuery(
                "select id, ts from {table} where id=@id;".format(
                    table=tabname),
                {'id': 2}
            )
            assert(len(result) > 0)
            assert(result[0] == (2, tstest))
