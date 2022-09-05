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

import grpc
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

from immudb import grpcutils
from immudb.handler import (batchGet, batchSet, changePassword, changePermission, createUser,
                            currentRoot, createDatabase, databaseList, deleteKeys, useDatabase,
                            get, listUsers, sqldescribe, verifiedGet, verifiedSet, setValue, history,
                            scan, reference, verifiedreference, zadd, verifiedzadd,
                            zscan, healthcheck, health, txbyid, verifiedtxbyid, sqlexec, sqlquery,
                            listtables, execAll, transaction)
from immudb.rootService import *
from immudb.grpc import schema_pb2_grpc
import warnings
import ecdsa
from immudb.datatypes import DeleteKeysRequest
from immudb.embedded.store import KVMetadata
import threading
import queue

import datetime


class ImmudbClient:

    def __init__(self, immudbUrl=None, rs: RootService = None, publicKeyFile: str = None, timeout=None):
        """Immudb client

        Args:
            immudbUrl (str, optional): url in format host:port, ex. localhost:3322 pointing to your immudb instance. Defaults to None.
            rs (RootService, optional): object that implements RootService - to be allow to verify requests. Defaults to None.
            publicKeyFile (str, optional): path to the public key that would be used to authenticate requests with. Defaults to None.
            timeout (int, optional): global timeout for GRPC requests, if None - it would hang until server respond. Defaults to None.
        """
        if immudbUrl is None:
            immudbUrl = "localhost:3322"
        self.timeout = timeout
        self.channel = grpc.insecure_channel(immudbUrl)
        self._resetStub()
        if rs is None:
            self.__rs = RootService()
        else:
            self.__rs = rs
        self.__url = immudbUrl
        self.loadKey(publicKeyFile)
        self.__login_response = None
        self._session_response = None

    def loadKey(self, kfile: str):
        """Loads public key from path

        Args:
            kfile (str): key file path
        """
        if kfile is None:
            self.__vk = None
        else:
            with open(kfile) as f:
                self.__vk = ecdsa.VerifyingKey.from_pem(f.read())

    def shutdown(self):
        """Shutdowns client
        """
        self.channel.close()
        self.channel = None
        self.intercept_channel.close
        self.intercept_channel = None
        self.__rs = None

    def set_session_id_interceptor(self, openSessionResponse):
        sessionId = openSessionResponse.sessionID
        self.headersInterceptors = [
            grpcutils.header_adder_interceptor('sessionid', sessionId)]
        return self.get_intercepted_stub()

    def set_token_header_interceptor(self, response):
        try:
            token = response.token
        except AttributeError:
            token = response.reply.token
        self.headersInterceptors = [
            grpcutils.header_adder_interceptor(
                'authorization', "Bearer " + token
            )
        ]
        return self.get_intercepted_stub()

    def get_intercepted_stub(self):
        allInterceptors = self.headersInterceptors + self.clientInterceptors
        intercepted, newStub = grpcutils.get_intercepted_stub(
            self.channel, allInterceptors)
        self.intercept_channel = intercepted
        return newStub

    @property
    def stub(self):
        return self.__stub

# from here on same order as in Golang ImmuClient interface (pkg/client/client.go)

    # Not implemented: disconnect
    # Not implemented: isConnected
    # Not implemented: waitForHealthCheck
    def healthCheck(self):
        """Retrieves health response of immudb

        Returns:
            HealthResponse: contains status and version
        """
        return healthcheck.call(self.__stub, self.__rs)

    # Not implemented: connect
    def _convertToBytes(self, what):
        if (type(what) != bytes):
            return bytes(what, encoding='utf-8')
        return what

    def login(self, username, password, database=b"defaultdb"):
        """Logins into immudb

        Args:
            username (str): username
            password (str): password for user
            database (bytes, optional): database to switch to. Defaults to b"defaultdb".

        Raises:
            Exception: if user tries to login on shut down client

        Returns:
            LoginResponse: contains token and warning if any
        """
        convertedUsername = self._convertToBytes(username)
        convertedPassword = self._convertToBytes(password)
        convertedDatabase = self._convertToBytes(database)
        req = schema_pb2_grpc.schema__pb2.LoginRequest(
            user=convertedUsername, password=convertedPassword)
        try:
            self.__login_response = schema_pb2_grpc.schema__pb2.LoginResponse = \
                self.__stub.Login(
                    req
                )
        except ValueError as e:
            raise Exception(
                "Attempted to login on termninated client, channel has been shutdown") from e

        self.__stub = self.set_token_header_interceptor(self.__login_response)
        # Select database, modifying stub function accordingly
        request = schema_pb2_grpc.schema__pb2.Database(
            databaseName=convertedDatabase)
        resp = self.__stub.UseDatabase(request)
        self.__stub = self.set_token_header_interceptor(resp)

        self.__rs.init("{}/{}".format(self.__url, database), self.__stub)
        return self.__login_response

    def logout(self):
        """Logouts all sessions
        """
        self.__stub.Logout(google_dot_protobuf_dot_empty__pb2.Empty())
        self.__login_response = None
        self._resetStub()

    def _resetStub(self):
        self.headersInterceptors = []
        self.clientInterceptors = []
        if (self.timeout != None):
            self.clientInterceptors.append(
                grpcutils.timeout_adder_interceptor(self.timeout))
        self.__stub = schema_pb2_grpc.ImmuServiceStub(self.channel)
        self.__stub = self.get_intercepted_stub()

    def keepAlive(self):
        """Sends keep alive packet
        """
        self.__stub.KeepAlive(google_dot_protobuf_dot_empty__pb2.Empty())

    def openManagedSession(self, username, password, database=b"defaultdb", keepAliveInterval=60):
        """Opens managed session and returns ManagedSession object within you can manage SQL transactions


        example of usage:
        with client.openManagedSession(username, password) as session:
            session.newTx()

        Check handler/transaction.py

        Args:
            username (str): username
            password (str): password for user
            database (bytes, optional): name of database. Defaults to b"defaultdb".
            keepAliveInterval (int, optional): specifies how often keep alive packet should be sent. Defaults to 60.

        Returns:
            ManagedSession: managed Session object
        """
        class ManagedSession:
            def __init__(this, keepAliveInterval):
                this.keepAliveInterval = keepAliveInterval
                this.keepAliveStarted = False
                this.keepAliveProcess = None
                this.queue = queue.Queue()

            def manage(this):
                while this.keepAliveStarted:
                    try:
                        what = this.queue.get(True, this.keepAliveInterval)
                    except queue.Empty:
                        self.keepAlive()

            def __enter__(this):
                interface = self.openSession(username, password, database)
                this.keepAliveStarted = True
                this.keepAliveProcess = threading.Thread(target=this.manage)
                this.keepAliveProcess.start()
                return interface

            def __exit__(this, type, value, traceback):
                this.keepAliveStarted = False
                this.queue.put(b'0')
                self.closeSession()

        return ManagedSession(keepAliveInterval)

    def openSession(self, username, password, database=b"defaultdb"):
        """Opens unmanaged session. Unmanaged means that you have to send keep alive packets yourself.
        Managed session does it for you

        Args:
            username (str): username
            password (str): password
            database (bytes, optional): database name to switch to. Defaults to b"defaultdb".

        Returns:
            Tx: Tx object (handlers/transaction.py)
        """
        convertedUsername = self._convertToBytes(username)
        convertedPassword = self._convertToBytes(password)
        convertedDatabase = self._convertToBytes(database)
        req = schema_pb2_grpc.schema__pb2.OpenSessionRequest(
            username=convertedUsername,
            password=convertedPassword,
            databaseName=convertedDatabase
        )
        self._session_response = self.__stub.OpenSession(
            req)
        self.__stub = self.set_session_id_interceptor(self._session_response)
        return transaction.Tx(self.__stub, self._session_response, self.channel)

    def closeSession(self):
        """Closes unmanaged session
        """
        self.__stub.CloseSession(google_dot_protobuf_dot_empty__pb2.Empty())
        self._session_response = None
        self._resetStub()

    def createUser(self, user, password, permission, database):
        """Creates user specified in parameters

        Args:
            user (str): username
            password (str): password
            permission (int): permissions (constants.PERMISSION_X)
            database (str): database name

        """
        request = schema_pb2_grpc.schema__pb2.CreateUserRequest(
            user=bytes(user, encoding='utf-8'),
            password=bytes(password, encoding='utf-8'),
            permission=permission,
            database=database
        )
        return createUser.call(self.__stub, self.__rs, request)

    def listUsers(self):
        """Returns all users on database

        Returns:
            ListUserResponse: List containing all users
        """
        return listUsers.call(self.__stub, None)

    def changePassword(self, user, newPassword, oldPassword):
        """Changes password for user

        Args:
            user (str): username
            newPassword (str): new password
            oldPassword (str): old password

        """
        request = schema_pb2_grpc.schema__pb2.ChangePasswordRequest(
            user=bytes(user, encoding='utf-8'),
            newPassword=bytes(newPassword, encoding='utf-8'),
            oldPassword=bytes(oldPassword, encoding='utf-8')
        )
        return changePassword.call(self.__stub, self.__rs, request)

    def changePermission(self, action, user, database, permission):
        """Changes permission for user

        Args:
            action (int): GRANT or REVOKE - see constants/PERMISSION_GRANT
            user (str): username
            database (str): database name
            permission (int): permission to revoke/ grant - see constants/PERMISSION_GRANT

        Returns:
            _type_: _description_
        """
        return changePermission.call(self.__stub, self.__rs, action, user, database, permission)

    # Not implemented: updateAuthConfig
    # Not implemented: updateMTLSConfig

    # Not implemented: with[.*]

    # Not implemented: getServiceClient
    # Not implemented: getOptions
    # Not implemented: setupDialOptions

    def databaseList(self):
        """Returns database list

        Returns:
            list[str]: database names
        """
        dbs = databaseList.call(self.__stub, self.__rs, None)
        return [x.databaseName for x in dbs.dblist.databases]

    # Not implemented: databaseListV2

    def createDatabase(self, dbName: bytes):
        """Creates database

        Args:
            dbName (bytes): name of database

        """
        request = schema_pb2_grpc.schema__pb2.Database(databaseName=dbName)
        return createDatabase.call(self.__stub, self.__rs, request)

    # Not implemented: createDatabaseV2
    # Not implemented: loadDatabase
    # Not implemented: unloadDatabase
    # Not implemented: deleteDatabase

    def useDatabase(self, dbName: bytes):
        """Switches database

        Args:
            dbName (bytes): database name

        """
        request = schema_pb2_grpc.schema__pb2.Database(databaseName=dbName)
        resp = useDatabase.call(self.__stub, self.__rs, request)
        # modify header token accordingly
        self.__stub = self.set_token_header_interceptor(resp)
        self.__rs.init(dbName, self.__stub)
        return resp

    # Not implemented: updateDatabase
    # Not implemented: updateDatabaseV2
    # Not implemented: getDatabaseSettings
    # Not implemented: getDatabaseSettingsV2

    # Not implemented: setActiveUser

    # Not implemented: flushIndex

    def compactIndex(self):
        """Starts index compaction
        """
        self.__stub.CompactIndex(google_dot_protobuf_dot_empty__pb2.Empty())

    def health(self):
        """Retrieves health response of immudb

        Returns:
            HealthResponse: contains status and version
        """
        return health.call(self.__stub, self.__rs)

    def currentState(self):
        """Return current state of immudb (proof)

        Returns:
            State: state of immudb
        """
        return currentRoot.call(self.__stub, self.__rs, None)

    def set(self, key: bytes, value: bytes):
        """Sets key into value in database

        Args:
            key (bytes): key
            value (bytes): value

        Returns:
            SetResponse: response of request
        """
        return setValue.call(self.__stub, self.__rs, key, value)

    def verifiedSet(self, key: bytes, value: bytes):
        """Sets key into value in database, and additionally checks it with state saved before

        Args:
            key (bytes): key
            value (bytes): value

        Returns:
            SetResponse: response of request
        """
        return verifiedSet.call(self.__stub, self.__rs, key, value, self.__vk)

    def expireableSet(self, key: bytes, value: bytes, expiresAt: datetime.datetime):
        """Sets key into value in database with additional expiration

        Args:
            key (bytes): key
            value (bytes): value
            expiresAt (datetime.datetime): Expiration time

        Returns:
            SetResponse: response of request
        """
        metadata = KVMetadata()
        metadata.ExpiresAt(expiresAt)
        return setValue.call(self.__stub, self.__rs, key, value, metadata)

    def get(self, key: bytes, atRevision: int = None):
        """Gets value for key

        Args:
            key (bytes): key
            atRevision (int, optional): gets value at revision specified by this argument. It could be relative (-1, -2), or fixed (32). Defaults to None.

        Returns:
            GetResponse: contains tx, value, key and revision
        """
        return get.call(self.__stub, self.__rs, key, atRevision=atRevision)

    # Not implemented: getSince
    # Not implemented: getAt

    def verifiedGet(self, key: bytes, atRevision: int = None):
        return verifiedGet.call(self.__stub, self.__rs, key, verifying_key=self.__vk, atRevision=atRevision)

    def verifiedGetSince(self, key: bytes, sinceTx: int):
        return verifiedGet.call(self.__stub, self.__rs, key, sinceTx=sinceTx, verifying_key=self.__vk)

    def verifiedGetAt(self, key: bytes, atTx: int):
        return verifiedGet.call(self.__stub, self.__rs, key, atTx, self.__vk)

    def history(self, key: bytes, offset: int, limit: int, sortorder: bool):
        return history.call(self.__stub, self.__rs, key, offset, limit, sortorder)

    def zAdd(self, zset: bytes, score: float, key: bytes, atTx: int = 0):
        return zadd.call(self.__stub, self.__rs, zset, score, key, atTx)

    def verifiedZAdd(self, zset: bytes, score: float, key: bytes, atTx: int = 0):
        return verifiedzadd.call(self.__stub, self.__rs, zset, score, key, atTx, self.__vk)

    # Not implemented: zAddAt
    # Not implemented: verifiedZAddAt

    def scan(self, key: bytes, prefix: bytes, desc: bool, limit: int, sinceTx: int = None):
        return scan.call(self.__stub, self.__rs, key, prefix, desc, limit, sinceTx)

    def zScan(self, zset: bytes, seekKey: bytes, seekScore: float,
              seekAtTx: int, inclusive: bool, limit: int, desc: bool, minscore: float,
              maxscore: float, sinceTx=None, nowait=False):
        return zscan.call(self.__stub, self.__rs, zset, seekKey, seekScore,
                          seekAtTx, inclusive, limit, desc, minscore,
                          maxscore, sinceTx, nowait)

    def txById(self, tx: int):
        return txbyid.call(self.__stub, self.__rs, tx)

    def verifiedTxById(self, tx: int):
        return verifiedtxbyid.call(self.__stub, self.__rs, tx, self.__vk)

    # Not implemented: txByIDWithSpec

    # Not implemented: txScan

    # Not implemented: count
    # Not implemented: countAll

    def setAll(self, kv: dict):
        return batchSet.call(self.__stub, self.__rs, kv)

    def getAll(self, keys: list):
        resp = batchGet.call(self.__stub, self.__rs, keys)
        return {key: value.value for key, value in resp.items()}

    def delete(self, req: DeleteKeysRequest):
        return deleteKeys.call(self.__stub, req)

    def execAll(self, ops: list, noWait=False):
        return execAll.call(self.__stub, self.__rs, ops, noWait)

    def setReference(self, referredkey: bytes, newkey:  bytes):
        return reference.call(self.__stub, self.__rs, referredkey, newkey)

    def verifiedSetReference(self, referredkey: bytes, newkey:  bytes):
        return verifiedreference.call(self.__stub, self.__rs, referredkey, newkey, verifying_key=self.__vk)

    # Not implemented: setReferenceAt
    # Not implemented: verifiedSetReferenceAt

    # Not implemented: dump

    # Not implemented: stream[.*]

    # Not implemented: exportTx
    # Not implemented: replicateTx

    def sqlExec(self, stmt, params={}, noWait=False):
        """Executes an SQL statement
        Args:
            stmt: a statement in immudb SQL dialect.
            params: a dictionary of parameters to replace in the statement
            noWait: whether to wait for indexing. Set to True for fast inserts.

        Returns:
            An object with two lists: ctxs and dtxs, including transaction
            metadata for both the catalog and the data store.

            Each element of both lists contains an object with the Transaction ID
            (id), timestamp (ts), and number of entries (nentries).
        """

        return sqlexec.call(self.__stub, self.__rs, stmt, params, noWait)

    def sqlQuery(self, query, params={}, columnNameMode=constants.COLUMN_NAME_MODE_NONE):
        """Queries the database using SQL
        Args:
            query: a query in immudb SQL dialect.
            params: a dictionary of parameters to replace in the query

        Returns:
            A list of table names. For example:

            ['table1', 'table2']
        """
        return sqlquery.call(self.__stub, self.__rs, query, params, columnNameMode)

    def listTables(self):
        """List all tables in the current database

        Returns:
            A list of table names. For example:

            ['table1', 'table2']
        """
        return listtables.call(self.__stub, self.__rs)

    def describeTable(self, table):
        return sqldescribe.call(self.__stub, self.__rs, table)

    # Not implemented: verifyRow

# deprecated
    def databaseCreate(self, dbName: bytes):
        warnings.warn("Call to deprecated databaseCreate. Use createDatabase instead",
                      category=DeprecationWarning,
                      stacklevel=2
                      )
        return self.createDatabase(dbName)

    def safeGet(self, key: bytes):  # deprecated
        warnings.warn("Call to deprecated safeGet. Use verifiedGet instead",
                      category=DeprecationWarning,
                      stacklevel=2
                      )
        return verifiedGet.call(self.__stub, self.__rs, key, verifying_key=self.__vk)

    def databaseUse(self, dbName: bytes):  # deprecated
        warnings.warn("Call to deprecated databaseUse. Use useDatabase instead",
                      category=DeprecationWarning,
                      stacklevel=2
                      )
        return self.useDatabase(dbName)

    def safeSet(self, key: bytes, value: bytes):  # deprecated
        warnings.warn("Call to deprecated safeSet. Use verifiedSet instead",
                      category=DeprecationWarning,
                      stacklevel=2
                      )
        return verifiedSet.call(self.__stub, self.__rs, key, value)


# immudb-py only


    def getAllValues(self, keys: list):  # immudb-py only
        resp = batchGet.call(self.__stub, self.__rs, keys)
        return resp

    def getValue(self, key: bytes):  # immudb-py only
        ret = get.call(self.__stub, self.__rs, key)
        if ret is None:
            return None
        return ret.value
