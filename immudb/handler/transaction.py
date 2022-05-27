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


from immudb.grpc import schema_pb2_grpc
from immudb import datatypes
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from immudb import constants
from immudb import grpcutils
from immudb.handler.sqlquery import _call_with_executor as executeSQLQuery
from immudb.handler.sqlexec import _call_with_executor as executeSQLExec


class Tx:
    def __init__(self, stub, session, channel):
        self.stub = stub
        self.session = session
        self.channel = channel
        self.txStub = None

    def makeTransactionInterceptedStub(self, transactionResponse):
        transactionId = transactionResponse.transactionID
        sessionId = self.session.sessionID
        headersInterceptors = [grpcutils.header_adder_interceptor(
            'sessionid', sessionId), grpcutils.header_adder_interceptor('transactionid', transactionId)]
        interceptedChannel, stub = grpcutils.get_intercepted_stub(
            self.channel, headersInterceptors)
        return stub

    def newTx(self, mode=datatypes.TxMode.ReadWrite):
        req = schema_pb2_grpc.schema__pb2.NewTxRequest(mode=mode)
        resp = self.stub.NewTx(req)
        self.txStub = self.makeTransactionInterceptedStub(resp)
        return self

    def commit(self):
        resp = self.txStub.Commit(google_dot_protobuf_dot_empty__pb2.Empty())
        self.txStub = None
        return resp

    def rollback(self):
        resp = self.txStub.Rollback(google_dot_protobuf_dot_empty__pb2.Empty())
        self.txStub = None
        return resp

    def sqlQuery(self, query, params=dict(), columnNameMode=constants.COLUMN_NAME_MODE_NONE):
        return executeSQLQuery(query, params, columnNameMode, self.txStub.TxSQLQuery)

    def sqlExec(self, stmt, params=dict(), noWait=False):
        return executeSQLExec(stmt, params, noWait, self.txStub.TxSQLExec)
