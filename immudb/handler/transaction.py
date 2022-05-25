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


from immudb.grpc import schema_pb2_grpc
from immudb import datatypes
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from immudb import constants
from immudb.handler.sqlquery import _call_with_executor as executeSQLQuery
from immudb.handler.sqlexec import _call_with_executor  as executeSQLExec

class InteractiveTxInterface:
    def __init__(self, client):
        self.client = client
    
    def newTx(self, mode = datatypes.TxMode.ReadWrite):
        req = schema_pb2_grpc.schema__pb2.NewTxRequest(mode = mode)
        resp = self.client._getStub().NewTx(req)
        self.client._setStub(self.client.set_transaction_id_interceptor(resp))
        return self

    def commit(self):
        self.client._getStub().Commit(google_dot_protobuf_dot_empty__pb2.Empty())

    def rollback(self):
        self.client._getStub().Rollback(google_dot_protobuf_dot_empty__pb2.Empty())

    def sqlQuery(self, query, params, columnNameMode = constants.COLUMN_NAME_MODE_NONE):
        return executeSQLQuery(query, params, columnNameMode, self.client._getStub().TxSQLQuery)

    def sqlExec(self, stmt, params = dict(), noWait = False):
        return executeSQLExec(stmt, params, noWait, self.client._getStub().TxSQLExec)
