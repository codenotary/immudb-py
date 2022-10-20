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

from io import BytesIO
from typing import Dict, Generator, List, Tuple, Union
import grpc
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2

from immudb import grpcutils
from immudb import datatypes
from immudb.grpc.schema_pb2 import Chunk, TxHeader
from immudb.handler import (batchGet, batchSet, changePassword, changePermission, createUser,
                            currentRoot, createDatabase, databaseList, deleteKeys, useDatabase,
                            get, listUsers, sqldescribe, verifiedGet, verifiedSet, setValue, history,
                            scan, reference, verifiedreference, zadd, verifiedzadd,
                            zscan, healthcheck, health, txbyid, verifiedtxbyid, sqlexec, sqlquery,
                            listtables, execAll, transaction, verifiedSQLGet)
from immudb.rootService import *
from immudb.grpc import schema_pb2_grpc
import warnings
import ecdsa
from immudb.datatypes import DeleteKeysRequest
from immudb.embedded.store import KVMetadata
import threading
import queue
import immudb.datatypesv2 as datatypesv2
import immudb.dataconverter as dataconverter

import datetime

from immudb.streamsutils import KeyHeader, StreamReader, ValueChunk, ValueChunkHeader, BufferedStreamReader


class ImmudbClient:

    def __init__(self, immudUrl=None, rs: RootService = None, publicKeyFile: str = None, timeout=None, max_grpc_message_length = None):
        """Immudb client

        Args:
            immudbUrl (str, optional): url in format host:port, ex. localhost:3322 pointing to your immudb instance. Defaults to None.
            rs (RootService, optional): object that implements RootService - to be allow to verify requests. Defaults to None.
            publicKeyFile (str, optional): path to the public key that would be used to authenticate requests with. Defaults to None.
            timeout (int, optional): global timeout for GRPC requests, if None - it would hang until server respond. Defaults to None.
            max_grpc_message_length (int, optional): max size for message from the server. If None - it would set defaults (4mb).
        """
        if immudUrl is None:
            immudUrl = "localhost:3322"
        self.timeout = timeout
        options = []
        if max_grpc_message_length:
            options = [('grpc.max_receive_message_length', max_grpc_message_length)]
            self.channel = grpc.insecure_channel(immudUrl, options = options)
        else:
            self.channel = grpc.insecure_channel(immudUrl)
        self._resetStub()
        if rs is None:
            self._rs = RootService()
        else:
            self._rs = rs
        self._url = immudUrl
        self._vk = None
        if publicKeyFile:
            self.loadKey(publicKeyFile)

    def loadKey(self, kfile: str):
        """Loads public key from path

        Args:
            kfile (str): key file path
        """
        with open(kfile) as f:
            self._vk = ecdsa.VerifyingKey.from_pem(f.read())
    def loadKeyFromString(self, key: str):
        """Loads public key from parameter

        Args:
            key (str): key 
        """
        self._vk = ecdsa.VerifyingKey.from_pem(key)

    def shutdown(self):
        """Shutdowns client
        """
        self.channel.close()
        self.channel = None
        self.intercept_channel.close
        self.intercept_channel = None
        self._rs = None

    def _set_session_id_interceptor(self, openSessionResponse):
        """Helper function to set session id interceptor

        Args:
            openSessionResponse (OpenSessionresponse): session response

        Returns:
            Stub: Intercepted stub
        """
        sessionId = openSessionResponse.sessionID
        self.headersInterceptors = [
            grpcutils.header_adder_interceptor('sessionid', sessionId)]
        return self._get_intercepted_stub()

    def _set_token_header_interceptor(self, response):
        """Helper function that sets token header interceptor

        Args:
            response (LoginResponse): login response

        Returns:
            Stub: Intercepted stub
        """
        try:
            token = response.token
        except AttributeError:
            token = response.reply.token
        self.headersInterceptors = [
            grpcutils.header_adder_interceptor(
                'authorization', "Bearer " + token
            )
        ]
        return self._get_intercepted_stub()

    def _get_intercepted_stub(self):
        """Helper function that returns intercepted stub

        Returns:
            Stub: Intercepted stub
        """
        allInterceptors = self.headersInterceptors + self.clientInterceptors
        intercepted, newStub = grpcutils.get_intercepted_stub(
            self.channel, allInterceptors)
        self.intercept_channel = intercepted
        return newStub

    @property
    def stub(self):
        return self._stub

    def healthCheck(self):
        """Retrieves health response of immudb

        Returns:
            HealthResponse: contains status and version
        """
        return healthcheck.call(self._stub, self._rs)

    def _convertToBytes(self, what):
        """Helper function that converts something to bytes with utf-8 encoding

        Args:
            what (UTF-8Encodable): Something that could be convert into utf-8

        Returns:
            bytes: Converted object
        """
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
        login_response = None
        try:
            login_response = schema_pb2_grpc.schema__pb2.LoginResponse = \
                self._stub.Login(
                    req
                )
        except ValueError as e:
            raise Exception(
                "Attempted to login on termninated client, channel has been shutdown") from e

        self._stub = self._set_token_header_interceptor(login_response)
        # Select database, modifying stub function accordingly
        request = schema_pb2_grpc.schema__pb2.Database(
            databaseName=convertedDatabase)
        resp = self._stub.UseDatabase(request)
        self._stub = self._set_token_header_interceptor(resp)

        self._rs.init("{}/{}".format(self._url, database), self._stub)
        return login_response

    def logout(self):
        """Logouts all sessions
        """
        self._stub.Logout(google_dot_protobuf_dot_empty__pb2.Empty())
        self._resetStub()

    def _resetStub(self):
        self.headersInterceptors = []
        self.clientInterceptors = []
        if (self.timeout != None):
            self.clientInterceptors.append(
                grpcutils.timeout_adder_interceptor(self.timeout))
        self._stub = schema_pb2_grpc.ImmuServiceStub(self.channel)
        self._stub = self._get_intercepted_stub()

    def keepAlive(self):
        """Sends keep alive packet
        """
        self._stub.KeepAlive(google_dot_protobuf_dot_empty__pb2.Empty())

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
        session_response = self._stub.OpenSession(
            req)
        self._stub = self._set_session_id_interceptor(session_response)
        return transaction.Tx(self._stub, session_response, self.channel)

    def closeSession(self):
        """Closes unmanaged session
        """
        self._stub.CloseSession(google_dot_protobuf_dot_empty__pb2.Empty())
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
        return createUser.call(self._stub, self._rs, request)

    def listUsers(self):
        """Returns all users on database

        Returns:
            ListUserResponse: List containing all users
        """
        return listUsers.call(self._stub)

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
        return changePassword.call(self._stub, self._rs, request)

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
        return changePermission.call(self._stub, self._rs, action, user, database, permission)

    def databaseList(self):
        """Returns database list

        Returns:
            list[str]: database names
        """
        dbs = databaseList.call(self._stub, self._rs, None)
        return [x.databaseName for x in dbs.dblist.databases]

    def createDatabase(self, dbName: bytes):
        """Creates database

        Args:
            dbName (bytes): name of database

        """
        request = schema_pb2_grpc.schema__pb2.Database(databaseName=dbName)
        return createDatabase.call(self._stub, self._rs, request)

    def createDatabaseV2(self, name: str, settings: datatypesv2.DatabaseSettingsV2, ifNotExists: bool) -> datatypesv2.CreateDatabaseResponseV2:
        """Creates database with settings

        Args:
            name (str): Name of database
            settings (datatypesv2.DatabaseSettingsV2): Settings of database
            ifNotExists (bool): would only create database if it not exist

        Returns:
            datatypesv2.CreateDatabaseResponseV2: Response contains information about new database
        """
        request = datatypesv2.CreateDatabaseRequest(name = name, settings = settings, ifNotExists = ifNotExists)
        resp = self._stub.CreateDatabaseV2(request._getGRPC())
        return dataconverter.convertResponse(resp)

    def databaseListV2(self) -> datatypesv2.DatabaseListResponseV2:
        """Lists databases

        Returns:
            datatypesv2.DatabaseListResponseV2: List of databases
        """
        req = datatypesv2.DatabaseListRequestV2()
        resp = self._stub.DatabaseListV2(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def loadDatabase(self, database: str) -> datatypesv2.LoadDatabaseResponse:
        """Loads database provided with argument

        Args:
            database (str): Name of database

        Returns:
            datatypesv2.LoadDatabaseResponse: Contains name of just loaded database
        """
        req = datatypesv2.LoadDatabaseRequest(database)
        resp = self._stub.LoadDatabase(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def unloadDatabase(self, database: str) -> datatypesv2.UnloadDatabaseResponse:
        """Unloads provided database

        Args:
            database (str): Name of database

        Returns:
            datatypesv2.UnloadDatabaseResponse: Contains name of just unloaded database
        """
        req = datatypesv2.UnloadDatabaseRequest(database)
        resp = self._stub.UnloadDatabase(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def deleteDatabase(self, database: str) -> datatypesv2.DeleteDatabaseResponse:
        """Deletes database provided with argument. Database needs to be unloaded first

        Args:
            database (str): Name of database

        Returns:
            datatypesv2.DeleteDatabaseResponse: Contains name of just deleted database
        """
        req = datatypesv2.DeleteDatabaseResponse(database)
        resp = self._stub.DeleteDatabase(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def updateDatabaseV2(self, database: str, settings: datatypesv2.DatabaseSettingsV2) -> datatypesv2.UpdateDatabaseResponseV2:
        """Updates database with provided argument

        Args:
            database (str): Name of database    
            settings (datatypesv2.DatabaseSettingsV2): Settings of database

        Returns:
            datatypesv2.UpdateDatabaseResponseV2: Contains name and settings of this database
        """
        request = datatypesv2.UpdateDatabaseRequest(database, settings)
        resp = self._stub.UpdateDatabaseV2(request._getGRPC())
        return dataconverter.convertResponse(resp)

    def useDatabase(self, dbName: bytes):
        """Switches database

        Args:
            dbName (bytes): database name

        """
        request = schema_pb2_grpc.schema__pb2.Database(databaseName=dbName)
        resp = useDatabase.call(self._stub, self._rs, request)
        # modifies header token accordingly
        self._stub = self._set_token_header_interceptor(resp)
        self._rs.init(dbName, self._stub)
        return resp


    def getDatabaseSettingsV2(self) -> datatypesv2.DatabaseSettingsResponseV2:
        """Returns current database settings

        Returns:
            datatypesv2.DatabaseSettingsResponseV2: Contains current database name and settings
        """
        req = datatypesv2.DatabaseSettingsRequest()
        resp = self._stub.GetDatabaseSettingsV2(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def setActiveUser(self, active: bool, username: str) -> bool:
        """Sets user as active or not active

        Args:
            active (bool): Should user be active
            username (str): Username of user

        Returns:
            bool: True if action was success
        """
        req = datatypesv2.SetActiveUserRequest(active, username)
        resp = self._stub.SetActiveUser(req._getGRPC())
        return resp == google_dot_protobuf_dot_empty__pb2.Empty()

    def flushIndex(self, cleanupPercentage: float, synced: bool) -> datatypesv2.FlushIndexResponse:
        """Routine that creates a fresh index based on the current state, removing all intermediate data generated over time

        Args:
            cleanupPercentage (float): Indicates how much space will be scanned for unreferenced data. Even though this operation blocks transaction processing, choosing a small percentage e.g. 0.1 may not significantly hinder normal operations while reducing used storage space.
            synced (bool): If true, fsync after writing data to avoid index regeneration in the case of an unexpected crash

        Returns:
            datatypesv2.FlushIndexResponse: Contains database name
        """
        req = datatypesv2.FlushIndexRequest(cleanupPercentage, synced)
        resp = self._stub.FlushIndex(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def compactIndex(self):
        """Starts full async index compaction - Routine that creates a fresh index based on the current state, removing all intermediate data generated over time
        """
        resp = self._stub.CompactIndex(google_dot_protobuf_dot_empty__pb2.Empty())
        return resp == google_dot_protobuf_dot_empty__pb2.Empty()

    def health(self):
        """Retrieves health response of immudb

        Returns:
            HealthResponse: contains status and version
        """
        return health.call(self._stub, self._rs)

    def currentState(self) -> State:
        """Return current state of immudb (proof)

        Returns:
            State: state of immudb
        """
        return currentRoot.call(self._stub, self._rs, None)

    def set(self, key: bytes, value: bytes) -> datatypes.SetResponse:
        """Sets key into value in database

        Args:
            key (bytes): key
            value (bytes): value

        Returns:
            SetResponse: response of request
        """
        return setValue.call(self._stub, self._rs, key, value)

    def verifiedSet(self, key: bytes, value: bytes) -> datatypes.SetResponse:
        """Sets key into value in database, and additionally checks it with state saved before

        Args:
            key (bytes): key
            value (bytes): value

        Returns:
            SetResponse: response of request
        """
        return verifiedSet.call(self._stub, self._rs, key, value, self._vk)

    def expireableSet(self, key: bytes, value: bytes, expiresAt: datetime.datetime) -> datatypes.SetResponse:
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
        return setValue.call(self._stub, self._rs, key, value, metadata)

    def get(self, key: bytes, atRevision: int = None) -> datatypes.GetResponse:
        """Gets value for key

        Args:
            key (bytes): key
            atRevision (int, optional): gets value at revision specified by this argument. It could be relative (-1, -2), or fixed (32). Defaults to None.

        Returns:
            GetResponse: contains tx, value, key and revision
        """
        return get.call(self._stub, self._rs, key, atRevision=atRevision)

    def verifiedGet(self, key: bytes, atRevision: int = None) -> datatypes.SafeGetResponse:
        """Get value for key and compares with saved state

        Args:
            key (bytes): Key to retrieve
            atRevision (int, optional): Retrieve key at desired revision. -1, -2... -n to get relative revision. Defaults to None.

        Returns:
            datatypes.SafeGetResponse: object that contains informations about transaction and verified state
        """
        return verifiedGet.call(self._stub, self._rs, key, verifying_key=self._vk, atRevision=atRevision)

    def verifiedGetSince(self, key: bytes, sinceTx: int) -> datatypes.SafeGetResponse:
        """Get value for key since tx (immudb will wait that the transaction specified by sinceTx is processed)

        Args:
            key (bytes): Key to retrieve
            sinceTx (int): transaction id (immudb will wait that the transaction specified by sinceTx is processed)

        Returns:
            datatypes.SafeGetResponse: object that contains informations about transaction and verified state
        """
        return verifiedGet.call(self._stub, self._rs, key, sinceTx=sinceTx, verifying_key=self._vk)

    def verifiedGetAt(self, key: bytes, atTx: int) -> datatypes.SafeGetResponse:
        """Get value at specified transaction

        Args:
            key (bytes): key to retrieve
            atTx (int): at transaction point

        Returns:
            datatypes.SafeGetResponse: object that contains informations about transaction and verified state
        """
        return verifiedGet.call(self._stub, self._rs, key, atTx, self._vk)

    def history(self, key: bytes, offset: int, limit: int, sortorder: bool) -> List[datatypes.historyResponseItem]:
        """Returns history of key

        Args:
            key (bytes): Key to retrieve
            offset (int): Offset of history
            limit (int): Limit of history entries
            sortorder (bool): Sort order of history

        Returns:
            List[datatypes.historyResponseItem]: List of history response items
        """
        return history.call(self._stub, self._rs, key, offset, limit, sortorder)

    def zAdd(self, zset: bytes, score: float, key: bytes, atTx: int = 0) -> datatypes.SetResponse:
        """Adds score (secondary index) for a specified key and collection

        Args:
            zset (bytes): collection name
            score (float): score
            key (bytes): key name
            atTx (int, optional): transaction id to bound score to. Defaults to 0 - current transaction

        Returns:
            datatypes.SetResponse: Set response contains transaction id 
        """
        return zadd.call(self._stub, self._rs, zset, score, key, atTx)

    def verifiedZAdd(self, zset: bytes, score: float, key: bytes, atTx: int = 0):
        """Adds score (secondary index) for a specified key and collection.
        Additionaly checks immudb state

        Args:
            zset (bytes): collection name
            score (float): score
            key (bytes): key name
            atTx (int, optional): transaction id to bound score to. Defaults to 0 - current transaction

        Returns:
            datatypes.SetResponse: Set response contains transaction id 
        """
        return verifiedzadd.call(self._stub, self._rs, zset, score, key, atTx, self._vk)

    def scan(self, key: bytes, prefix: bytes, desc: bool, limit: int, sinceTx: int = None) -> Dict[bytes, bytes]:
        """Scans for provided parameters. Limit for scan is fixed - 1000. You need to introduce pagination.

        Args:
            key (bytes): Seek key to find
            prefix (bytes): Prefix of key
            desc (bool): Descending or ascending order
            limit (int): Limit of entries to get
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.

        Returns:
            Dict[bytes, bytes]: Dictionary of key and values
        """
        return scan.call(self._stub, self._rs, key, prefix, desc, limit, sinceTx)

    def zScan(self, zset: bytes, seekKey: bytes, seekScore: float,
              seekAtTx: int, inclusive: bool, limit: int, desc: bool, minscore: float,
              maxscore: float, sinceTx=None, nowait=False) -> schema_pb2.ZEntries:
        """Scan for provided parameters for secondary index. Limit for scan is fixed - 1000. You need to introduce pagination.

        Args:
            zset (bytes): Set name
            seekKey (bytes): Seek key to find
            seekScore (float): Seek score - min or max score for entry (depending on desc value)
            seekAtTx (int): Tx id for the first entry
            inclusive (bool): Element resulting from seek key would be part of resulting set
            limit (int): Maximum number of returned items
            desc (bool): Descending or ascending order
            minscore (float): Min score
            maxscore (float): Max score
            sinceTx (_type_, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            nowait (bool, optional): when true - scan doesn't wait for transaction at seekAtTx to be procesessed. Defaults to False.

        Returns:
            schema_pb2.ZEntries: Entries of this scan
        """
        return zscan.call(self._stub, self._rs, zset, seekKey, seekScore,
                          seekAtTx, inclusive, limit, desc, minscore,
                          maxscore, sinceTx, nowait)

    def txById(self, tx: int) -> schema_pb2.Tx:
        """Returns keys list modified in transaction by transaction id

        Args:
            tx (int): transaction id

        Returns:
            List[bytes]: Keys list modified in queried transaction
        """
        return txbyid.call(self._stub, self._rs, tx)

    def verifiedTxById(self, tx: int):
        """Returns and verifies keys list modified in transaction by transaction id

        Args:
            tx (int): transaction id

        Returns:
            List[bytes]: Keys list modified in queried transaction
        """
        return verifiedtxbyid.call(self._stub, self._rs, tx, self._vk)

    def txScan(self, initialTx: int, limit: int = 999, desc: bool = False, entriesSpec: datatypesv2.EntriesSpec = None, sinceTx: int = 0, noWait: bool = False) -> datatypesv2.TxList:
        """Scans for transactions with specified parameters

        Args:
            initialTx (int): initial transaction id
            limit (int, optional): Limit resulsts. Defaults to 999.
            desc (bool, optional): Descending or ascending. Defaults to False.
            entriesSpec (datatypesv2.EntriesSpec, optional): Specified what should be contained in scan. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.

        Returns:
            datatypesv2.TxList: Transaction list
        """
        req = datatypesv2.TxScanRequest(initialTx, limit, desc, entriesSpec, sinceTx, noWait)
        resp = self._stub.TxScan(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def serverInfo(self) -> datatypesv2.ServerInfoResponse:
        """Returns server info containing version

        Returns:
            datatypesv2.ServerInfoResponse: Contains version of running server
        """
        req = datatypesv2.ServerInfoRequest()
        resp = self._stub.ServerInfo(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def databaseHealth(self) -> datatypesv2.DatabaseHealthResponse:
        """Returns information about database health (pending requests, last request completion timestamp)

        Returns:
            datatypesv2.DatabaseHealthResponse: Contains informations about database (pending requests, last request completion timestamp)
        """
        req = google_dot_protobuf_dot_empty__pb2.Empty()
        resp = self._stub.DatabaseHealth(req)
        return dataconverter.convertResponse(resp)

    def setAll(self, kv: Dict[bytes, bytes]) -> datatypes.SetResponse:
        """Sets all values for corresponding keys from dictionary

        Args:
            kv (Dict[bytes, bytes]): dictionary of keys and values

        Returns:
            datatypes.SetResponse: Set response contains transaction id 
        """
        return batchSet.call(self._stub, self._rs, kv)

    def getAll(self, keys: List[bytes]) -> Dict[bytes, bytes]:
        """Returns values for specified keys

        Args:
            keys (List[bytes]): Keys list 

        Returns:
            Dict[bytes, bytes]: Dictionary of key : value pairs
        """
        resp = batchGet.call(self._stub, self._rs, keys)
        return {key: value.value for key, value in resp.items()}

    def delete(self, req: DeleteKeysRequest) -> TxHeader:
        """Deletes key

        Args:
            req (DeleteKeysRequest): Request contains key to delete

        Returns:
            TxHeader: Transaction header
        """
        return deleteKeys.call(self._stub, req)

    def execAll(self, ops: List[Union[datatypes.KeyValue, datatypes.ZAddRequest, datatypes.ReferenceRequest]], noWait=False) -> TxHeader:
        """Exectues all operations from list (KeyValue, ZAddRequest, ReferenceRequest)

        Args:
            ops (List[Union[datatypes.KeyValue, datatypes.ZAddRequest, datatypes.ReferenceRequest]]): List of operations
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None. Defaults to False.

        Returns:
            TxHeader: Transaction header
        """
        return execAll.call(self._stub, self._rs, ops, noWait)

    def setReference(self, referredkey: bytes, newkey:  bytes) -> TxHeader:
        """References key specified by referredkey as newkey

        Args:
            referredkey (bytes): Reffered key
            newkey (bytes): New key

        Returns:
            TxHeader: Transaction header
        """
        return reference.call(self._stub, self._rs, referredkey, newkey)

    def verifiedSetReference(self, referredkey: bytes, newkey:  bytes) -> TxHeader:
        """References key specified by referredkey as newkey and verifies state of immudb

        Args:
            referredkey (bytes): Reffered key
            newkey (bytes): New key

        Returns:
            TxHeader: Transaction header
        """
        return verifiedreference.call(self._stub, self._rs, referredkey, newkey, verifying_key=self._vk)

    def _rawStreamGet(self, key: bytes, atTx: int = None, sinceTx: int = None, noWait: bool = None, atRevision: int = None) -> Generator[Union[KeyHeader, ValueChunk], None, None]:
        """Helper function that creates generator of chunks from raw GRPC stream

        Yields:
            Generator[Union[KeyHeader, ValueChunk], None, None]: First chunk is KeyHeader, rest are ValueChunks
        """
        req = datatypesv2.KeyRequest(key = key, atTx = atTx, sinceTx = sinceTx, noWait = noWait, atRevision = atRevision)
        resp = self._stub.streamGet(req._getGRPC())
        reader = StreamReader(resp)
        for it in reader.chunks():
            yield it

    def streamGet(self, key: bytes, atTx: int = None, sinceTx: int = None, noWait: bool = None, atRevision: int = None) -> Tuple[bytes, BufferedStreamReader]:
        """Streaming method to get buffered value. 
        You can read from this value by read() method
        read() will read everything
        read(256) will read 256 bytes

        Args:
            key (bytes): Key to get
            atTx (int, optional): Get key at transaction id. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.
            atRevision (int, optional): Returns value of key at specified revision. -1 to get relative revision. Defaults to None.

        Returns:
            Tuple[bytes, BufferedStreamReader]: First value is key, second is reader. 
        """
        req = datatypesv2.KeyRequest(key = key, atTx = atTx, sinceTx = sinceTx, noWait = noWait, atRevision = atRevision)
        resp = self._stub.streamGet(req._getGRPC())
        reader = StreamReader(resp)
        chunks = reader.chunks()
        keyHeader = next(chunks)
        valueHeader = next(chunks)
        return keyHeader.key, BufferedStreamReader(chunks, valueHeader, resp)

    def streamGetFull(self, key: bytes, atTx: int = None, sinceTx: int = None, noWait: bool = None, atRevision: int = None) -> datatypesv2.KeyValue:
        """Streaming method to get full value

        Args:
            key (bytes): Key to get
            atTx (int, optional): Get key at transaction id. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.
            atRevision (int, optional): Returns value of key at specified revision. -1 to get relative revision. Defaults to None.

        Returns:
            datatypesv2.KeyValue: Key value from immudb
        """
        req = datatypesv2.KeyRequest(key = key, atTx = atTx, sinceTx = sinceTx, noWait = noWait, atRevision = atRevision)
        resp = self._stub.streamGet(req._getGRPC())
        reader = StreamReader(resp)
        key = None
        value = b''
        chunks = reader.chunks()
        key = next(chunks).key
        for it in chunks:
            value += it.chunk
        return datatypesv2.KeyValue(key, value)

    def _make_set_stream(self, buffer, key: bytes, length: int, chunkSize: int = 65536):
        """Helper function that creates generator from buffer

        Args:
            buffer (io.BytesIO): Any buffer that implements read(length: int) method
            key (bytes): Key to set
            length (int): Length of buffer
            chunkSize (int, optional): Chunk size to set while streaming. Defaults to 65536.

        Yields:
            Generator[Chunk, None, None]: Chunk that is cmpatible with proto
        """
        yield Chunk(content = KeyHeader(key = key, length=len(key)).getInBytes())
        firstChunk = buffer.read(chunkSize)
        firstChunk = ValueChunkHeader(chunk = firstChunk, length = length).getInBytes()
        yield Chunk(content = firstChunk)
        chunk = buffer.read(chunkSize)
        while chunk:
            yield Chunk(content = chunk)
            chunk = buffer.read(chunkSize)

    def streamScan(self, seekKey: bytes = None, endKey: bytes = None, prefix: bytes = None, desc: bool = None, limit: int = None, sinceTx: int = None, noWait: bool = None, inclusiveSeek: bool = None, inclusiveEnd: bool = None, offset: int = None) -> Generator[datatypesv2.KeyValue, None, None]:
        """Scan method in streaming maneer

        Args:
            seekKey (bytes, optional): Key to seek. Defaults to None.
            endKey (bytes, optional): Key to end scan with. Defaults to None.
            prefix (bytes, optional): Key prefix. Defaults to None.
            desc (bool, optional): Sorting order - true to descending. Defaults to None.
            limit (int, optional): Limit of scan items. Defaults to None.
            sinceTx (int, optional): immudb will wait that the transaction specified by sinceTx is processed. Defaults to None.
            noWait (bool, optional): When true - scan doesn't wait for the index to be fully generated. Defaults to None.
            inclusiveSeek (bool, optional): Includes seek key value. Defaults to None.
            inclusiveEnd (bool, optional): Includes end key value also. Defaults to None.
            offset (int, optional): Offsets current scan. Defaults to None.

        Yields:
            Generator[datatypesv2.KeyValue, None, None]: Returns generator of KeyValue
        """
        req = datatypesv2.ScanRequest(seekKey=seekKey, endKey=endKey, prefix = prefix, desc = desc, limit = limit, sinceTx= sinceTx, noWait=noWait, inclusiveSeek=None, inclusiveEnd=None, offset=None)
        resp = self._stub.streamScan(req._getGRPC())
        key = None
        value = None
        for chunk in StreamReader(resp).chunks():
            if isinstance(chunk, KeyHeader):
                if key != None:
                    yield datatypesv2.KeyValue(key = key, value = value, metadata = None)
                key = chunk.key
                value = b''
            else:
                value += chunk.chunk

        if key != None and value != None: # situation when generator consumes all at first run, so it didn't yield first value
            yield datatypesv2.KeyValue(key = key, value = value, metadata = None)

    def streamScanBuffered(self, seekKey: bytes = None, endKey: bytes = None, prefix: bytes = None, desc: bool = None, limit: int = None, sinceTx: int = None, noWait: bool = None, inclusiveSeek: bool = None, inclusiveEnd: bool = None, offset: int = None) -> Generator[Tuple[bytes, BufferedStreamReader], None, None]:
        """Scan method in streaming maneer. Differs from streamScan with method to read from buffer also.
        Useful in case of big values.

        Important - you can't skip reading from any buffer. You always need to consume it

        Args:
            seekKey (bytes, optional): Key to seek. Defaults to None.
            endKey (bytes, optional): Key to end scan with. Defaults to None.
            prefix (bytes, optional): Key prefix. Defaults to None.
            desc (bool, optional): Sorting order - true to descending. Defaults to None.
            limit (int, optional): Limit of scan items. Defaults to None.
            sinceTx (int, optional): immudb will wait that the transaction specified by sinceTx is processed. Defaults to None.
            noWait (bool, optional): When true - scan doesn't wait for the index to be fully generated. Defaults to None.
            inclusiveSeek (bool, optional): Includes seek key value. Defaults to None.
            inclusiveEnd (bool, optional): Includes end key value also. Defaults to None.
            offset (int, optional): Offsets current scan. Defaults to None.

        Yields:
            Generator[Tuple[bytes, BufferedStreamReader], None, None]: First value is Key, second is buffer that you can read from
        """

        req = datatypesv2.ScanRequest(seekKey=seekKey, endKey=endKey, prefix = prefix, desc = desc, limit = limit, sinceTx= sinceTx, noWait=noWait, inclusiveSeek=inclusiveSeek, inclusiveEnd=inclusiveEnd, offset=offset)
        resp = self._stub.streamScan(req._getGRPC())
        key = None
        valueHeader = None

        streamReader = StreamReader(resp)
        chunks = streamReader.chunks()
        chunk = next(chunks)
        while chunk != None:
            if isinstance(chunk, KeyHeader):
                key = chunk.key
                valueHeader = next(chunks)
                yield key, BufferedStreamReader(chunks, valueHeader, resp)
            chunk = next(chunks, None)



    def _rawStreamSet(self, generator: Generator[Union[KeyHeader, ValueChunkHeader, ValueChunk], None, None]) -> datatypesv2.TxHeader:
        """Helper function that grabs generator of chunks and set into immudb

        Args:
            generator (Generator[Union[KeyHeader, ValueChunkHeader, ValueChunk], None, None]): Generator

        Returns:
            datatypesv2.TxHeader: Transaction header
        """
        resp = self._stub.streamSet(generator)
        return dataconverter.convertResponse(resp)

    def streamSet(self, key: bytes, buffer, bufferLength: int, chunkSize: int = 65536) -> datatypesv2.TxHeader:
        """Sets key into value with streaming method. 

        Args:
            key (bytes): Key
            buffer (io.BytesIO): Any buffer that implements read(length: int) method
            bufferLength (int): Buffer length (protocol needs to know it at first)
            chunkSize (int, optional): Specifies chunk size while sending. Defaults to 65536. Defaults to 65536.

        Returns:
            datatypesv2.TxHeader: Transaction header of just set transaction
        """
        resp = self._rawStreamSet(self._make_set_stream(buffer, key, bufferLength, chunkSize))
        return resp

    def streamSetFullValue(self, key: bytes, value: bytes, chunkSize: int = 65536) -> datatypesv2.TxHeader:
        """Sets key into value with streaming maneer. Differs from streamSet because user can set full value

        Args:
            key (bytes): Key to set
            value (bytes): Value to set
            chunkSize (int, optional): Specifies chunk size while sending. Defaults to 65536.

        Returns:
            datatypesv2.TxHeader: Transaction header
        """
        resp = self._rawStreamSet(self._make_set_stream(BytesIO(value), key, len(value), chunkSize))
        return resp

    def exportTx(self, tx: int):
        """Opens stream to export transaction from immudb (you can combine it with replicateTx)

        Args:
            tx (int): transaction id

        Returns:
            Generator[Chunk, None, None]: Iterable of chunk
        """
        return self._stub.exportTx(datatypesv2.ExportTxRequest(tx)._getGRPC())

    def _create_generator(self, chunkStream):
        """Helper function that creates generator from any iterable

        Args:
            chunkStream (Iterable[Chunk]): Iterable of Chunk

        Yields:
            Chunk: Chunk (from immudb schema_pb2)
        """
        for chunk in chunkStream:
            yield chunk

    def replicateTx(self, chunkStream) -> datatypesv2.TxHeader:
        """Replicates transaction provided by stream

        Args:
            chunkStream (Iterable[Chunk]): fixed list of chunk, or stream from exportTx method

        Returns:
            datatypesv2.TxHeader: tx header of just synchronized transaction
        """
        return dataconverter.convertResponse(self._stub.replicateTx(self._create_generator(chunkStream)))

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

        return sqlexec.call(self._stub, self._rs, stmt, params, noWait)

    def sqlQuery(self, query, params={}, columnNameMode=constants.COLUMN_NAME_MODE_NONE):
        """Queries the database using SQL
        Args:
            query: a query in immudb SQL dialect.
            params: a dictionary of parameters to replace in the query

        Returns:
            A list of table names. For example:

            ['table1', 'table2']
        """
        return sqlquery.call(self._stub, self._rs, query, params, columnNameMode)

    def listTables(self):
        """List all tables in the current database

        Returns:
            A list of table names. For example:

            ['table1', 'table2']
        """
        return listtables.call(self._stub, self._rs)

    def describeTable(self, table) -> List[datatypes.ColumnDescription]:
        """Describes table provided by argument

        Args:
            table (str): Table to describe

        Returns:
            List[datatypes.ColumnDescription]: Column descriptions
        """
        return sqldescribe.call(self._stub, self._rs, table)

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
        return verifiedGet.call(self._stub, self._rs, key, verifying_key=self._vk)

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
        return verifiedSet.call(self._stub, self._rs, key, value)

    def verifiableSQLGet(self, table, primaryKeys, atTx = None, sinceTx = None): 
        return verifiedSQLGet.call(self._stub, self._rs, table, primaryKeys, atTx, sinceTx, verifying_key=self._vk)


# immudb-py only


    def getAllValues(self, keys: list):  # immudb-py only
        resp = batchGet.call(self._stub, self._rs, keys)
        return resp

    def getValue(self, key: bytes):  # immudb-py only
        ret = get.call(self._stub, self._rs, key)
        if ret is None:
            return None
        return ret.value
