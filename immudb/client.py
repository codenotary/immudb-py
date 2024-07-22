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
from immudb.exceptions import ErrCorruptedData
from immudb.grpc.schema_pb2 import Chunk, TxHeader, ZAddRequest
from immudb.handler import (batchGet, batchSet, changePassword, changePermission, createUser,
                            currentRoot, createDatabase, databaseList, deleteKeys, useDatabase,
                            get, listUsers, sqldescribe, verifiedGet, verifiedSet, setValue, history,
                            scan, reference, verifiedreference, zadd, verifiedzadd,
                            zscan, healthcheck, health, txbyid, verifiedtxbyid, sqlexec, sqlquery,
                            listtables, execAll, transaction, verifiedSQLGet)

from immudb.handler.verifiedtxbyid import verify as verifyTransaction
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

from immudb.streamsutils import AtTXHeader, KeyHeader, ProvenSinceHeader, ScoreHeader, SetHeader, StreamReader, ValueChunk, ValueChunkHeader, BufferedStreamReader, VerifiedGetStreamReader, ZScanStreamReader


class ImmudbClient:

    def __init__(self, immudUrl=None, rs: RootService = None, publicKeyFile: str = None, timeout=None, max_grpc_message_length=None):
        """immudb Client

        Args:
            immudbUrl (str, optional): url in format ``host:port``
                (e.g. ``localhost:3322``) of your immudb instance.
                Defaults to ``localhost:3322`` when no value is set.
            rs (RootService, optional): object that implements RootService,
                allowing requests to be verified. Optional. 
                By default in-memory RootService instance will be created
            publicKeyFile (str, optional): path of the public key to use
                for authenticating requests. Optional.
            timeout (int, optional): global timeout for GRPC requests. Requests
                will hang until the server responds if no timeout is set.
            max_grpc_message_length (int, optional): maximum size of message the
                server should send. The default (4Mb) is used is no value is set.
        """
        if immudUrl is None:
            immudUrl = "localhost:3322"
        self.timeout = timeout
        options = []
        if max_grpc_message_length:
            options = [('grpc.max_receive_message_length',
                        max_grpc_message_length)]
            self.channel = grpc.insecure_channel(immudUrl, options=options)
        else:
            self.channel = grpc.insecure_channel(immudUrl)
        self._resetStub()
        if rs is None:
            self._rs = RootService()
        else:
            self._rs = rs
        self._url = immudUrl
        self._vk = None
        self._currentdb = None
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
        self._currentdb = convertedDatabase
        return login_response

    def logout(self):
        """Logouts all sessions
        """
        self._stub.Logout(google_dot_protobuf_dot_empty__pb2.Empty())
        self._resetStub()
        self._currentdb = None

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
        """Opens a session managed by immudb.

        When a ManagedSession is used, the client will automatically
        send keepalive packets to the server. If you are managing
        your application's threading yourself and want control over
        how keepalive packets are sent, consider the method
        :meth:`ImmudbClient.openSession()` instead.


        Examples:
            with client.openManagedSession(username, password) as session:
                session.newTx()

        Check handler/transaction.py

        Args:
            username (str): username
            password (str): password for user
            database (bytes, optional): database to establish session with.
                Defaults to ``b"defaultdb"``.
            keepAliveInterval (int, optional): specifies how often keepalive
                packets should be sent, in seconds. Defaults to ``60s``.

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
        """Opens unmanaged Session object.

        When a Session is unmanaged, it is the user's responsibility
        to send keepalive packets. You should use an unmanaged session
        when you are also managing your application's threading yourself.

        To have the client manage the session and send keepalive packets
        for you, use :meth:`ImmudbClient.openManagedSession()` instead.

        Args:
            username (str): username
            password (str): password
            database (bytes, optional): database to establish session with.
                Defaults to ``b"defaultdb"``.

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
        return transaction.Tx(self._stub, database, session_response, self.channel)

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
        """Returns all database users.

        Returns:
            ListUserResponse: List containing all database users.

            If this method is called by an authenticated Superadmin user,
            the ListUserResponse object will contain all known users.

            If this method is called by an authenticated Admin users,
            the response will contain the list of users for the
            current database.

        """
        return listUsers.call(self._stub)

    def changePassword(self, user, newPassword, oldPassword):
        """Changes password for user

        Args:
            user (str): username
            newPassword (str): new password
            oldPassword (str): old password

        Comment:
            SysAdmin can change his own password only by giving old and new password.
            SysAdmin user can change password of any other user without old password.
            Admin users can change password for user only created by that admin without old password.


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
            ifNotExists (bool): only create a database if it does not exist yet.

        Returns:
            datatypesv2.CreateDatabaseResponseV2: Response contains information about new database
        """
        request = datatypesv2.CreateDatabaseRequest(
            name=name, settings=settings, ifNotExists=ifNotExists)
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
        self._currentdb = dbName
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
        """Request a flush of the internal to disk, with the option to cleanup the index.

        This routine requests that the internal B-tree index be flushed from
        memory to disk, optionally followed by a cleanup of intermediate index data.

        Args:
            cleanupPercentage (float): Indicates the percentage of the
                storage space that will be scanned for unreference data. Although
                this operation blocks transaction processing, choosing a small
                value (e.g. ``0.1``) may not significantly hinder normal operations,
                and will reduce used storage space.
            synced (bool): If `True`, ``fsync`` will be called after writing data
                to avoid index regeneration in the event of an unexpected crash.

        Returns:
            datatypesv2.FlushIndexResponse: Contains database name
        """
        req = datatypesv2.FlushIndexRequest(cleanupPercentage, synced)
        resp = self._stub.FlushIndex(req._getGRPC())
        return dataconverter.convertResponse(resp)

    def compactIndex(self):
        """Start full async index compaction.

        This creates a fresh index representing the current state of
        the database, removing the intermediate index data generated over
        time that is no longer needed to represent the current state.

        The :meth:`ImmudbClient.flushIndex()` method (with a `cleanupPercentage`)
        argument specified) should be preferred over this method for compacting
        the index. You should only call this method if there's little to no activity
        on the database, or the performance of the database may be degraded
        significantly while the compaction is in progress.
        """
        resp = self._stub.CompactIndex(
            google_dot_protobuf_dot_empty__pb2.Empty())
        return resp == google_dot_protobuf_dot_empty__pb2.Empty()

    def health(self):
        """Retrieves health response of immudb

        Returns:
            HealthResponse: contains status and version
        """
        return health.call(self._stub, self._rs)

    def currentState(self) -> State:
        """Return current state (proof) of current database.

        Returns:
            State: State of current database, proving integrity of its data.
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
        """Get value for key.

        Args:
            key (bytes): Key of value to retrieve.
            atRevision (int, optional):
                Specify the revision from which the value should be retrieved.
                A negative integer value (e.g. ``-2``) specifies a revision relative
                to the current revision. A positive integer value (e.g. ``32``) should
                be used to represent a fixed revision number. If not specified, the
                most recent value will be returned.

        Returns:
            GetResponse: Contains `tx`, `value`, `key` and `revision` information.
        """
        return get.call(self._stub, self._rs, key, atRevision=atRevision)

    def verifiedGet(self, key: bytes, atRevision: int = None) -> datatypes.SafeGetResponse:
        """Get value for key and verify it against saved state.

        Args:
            key (bytes): Key of value to retrieve.
            atRevision (int, optional):
                Specify the revision from which the value should be retrieved.
                A negative integer value (e.g. ``-2``) specifies a revision relative
                to the current revision. A positive integer value (e.g. ``32``) should
                be used to represent a fixed revision number. If not specified, the
                most recent value will be returned.

        Returns:
            SafeGetResponse: Contains information about the transaction
                and the verified state.
        """
        return verifiedGet.call(self._stub, self._rs, key, verifying_key=self._vk, atRevision=atRevision)

    def verifiedGetSince(self, key: bytes, sinceTx: int) -> datatypes.SafeGetResponse:
        """Get value for key since a given transaction (and wait if that transaction is not yet indexed).

        This method retrieves the value for a given key, as of a given transaction
        number in the database. If the transaction specified by ``sinceTx`` has not
        yet been indexed by immudb, this method will block until it has been indexed.

        Args:
            key (bytes): Key of value to retrieve.
            sinceTx (int): Identifier of the earliest transaction from which the
                key's value should be retrieved. If the specified transaction has
                not been indexed by immudb, this method will block until it has.

        Returns:
            datatypes.SafeGetResponse: object that contains informations about transaction and verified state
        """
        return verifiedGet.call(self._stub, self._rs, key, sinceTx=sinceTx, verifying_key=self._vk)

    def verifiedGetAt(self, key: bytes, atTx: int) -> datatypes.SafeGetResponse:
        """Get value for key at a given transaction point.

        Args:
            key (bytes): Key of value to retrieve.
            atTx (int): Identifier of the transaction at which point the key's
                value should be retrieved.

        Returns:
            datatypes.SafeGetResponse: object that contains informations about transaction and verified state
        """
        return verifiedGet.call(self._stub, self._rs, key, atTx, self._vk)

    def history(self, key: bytes, offset: int, limit: int, sortorder: bool) -> List[datatypes.historyResponseItem]:
        """Returns history of values for a given key.

        Args:
            key (bytes): Key of value to retrieve.
            offset (int): Offset of history
            limit (int): Limit of history entries
            sortorder (bool, optional, deprecated): A boolean value that specifies if the history
                should be returned in descending order.

                If ``True``, the history will be returned in descending order,
                with the most recent value in the history being the first item in the list.

                If ``False``, the list will be sorted in ascending order,
                with the most recent value in the history being the last item in the list.

        Returns:
            List[datatypes.historyResponseItem]: List of history response items
        """
        return history.call(self._stub, self._rs, key, offset, limit, sortorder)

    def zAdd(self, zset: bytes, score: float, key: bytes, atTx: int = 0) -> datatypes.SetResponse:
        """Adds score (secondary index) for a specified key and collection.

        Args:
            zset (bytes): collection name
            score (float): score
            key (bytes): key name
            atTx (int, optional): Transaction id to bound score to. Defaults to 0,
                indicating the most recent version should be used.

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
            atTx (int, optional): transaction id to bound score to. Defaults to 0,
                indicating the most recent version should be used.

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

    def zScan(self, zset: bytes, seekKey: bytes = None, seekScore: float = None,
              seekAtTx: int = None, inclusive: bool = None, limit: int = None, desc: bool = None, minscore: float = None,
              maxscore: float = None, sinceTx=None, nowait=False) -> schema_pb2.ZEntries:
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
            desc (bool, optional): If `True`, use descending scan order.
                Defaults to `False`, which uses ascending scan order.
            entriesSpec (datatypesv2.EntriesSpec, optional): Specified what should be contained in scan. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.

        Returns:
            datatypesv2.TxList: Transaction list
        """
        req = datatypesv2.TxScanRequest(
            initialTx, limit, desc, entriesSpec, sinceTx, noWait)
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
        req = datatypesv2.KeyRequest(
            key=key, atTx=atTx, sinceTx=sinceTx, noWait=noWait, atRevision=atRevision)
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
        req = datatypesv2.KeyRequest(
            key=key, atTx=atTx, sinceTx=sinceTx, noWait=noWait, atRevision=atRevision)
        resp = self._stub.streamGet(req._getGRPC())
        reader = StreamReader(resp)
        chunks = reader.chunks()
        keyHeader = next(chunks, None)
        if keyHeader != None:
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
        req = datatypesv2.KeyRequest(
            key=key, atTx=atTx, sinceTx=sinceTx, noWait=noWait, atRevision=atRevision)
        resp = self._stub.streamGet(req._getGRPC())
        reader = StreamReader(resp)
        key = None
        value = b''
        chunks = reader.chunks()
        chunk = next(chunks, None)
        if chunk != None:
            key = chunk.key
            for it in chunks:
                value += it.chunk
            return datatypesv2.KeyValue(key, value)

    def streamVerifiedGet(self, key: bytes = None, atTx: int = None, sinceTx: int = None, noWait: bool = None, atRevision: int = None) -> datatypes.SafeGetResponse:
        """Gets a value of a key with streaming method, and verifies transaction.

        Args:
            key (bytes): Key to get
            atTx (int, optional): Get key at transaction id. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.
            atRevision (int, optional): Returns value of key at specified revision. -1 to get relative revision. Defaults to None.

        Raises:
            ErrCorruptedData: When data is corrupted or unverifiable

        Returns:
            datatypes.SafeGetResponse: Response contains informations about verification
        """
        state = self._rs.get()
        proveSinceTx = state.txId
        req = datatypesv2.VerifiableGetRequest(keyRequest=datatypesv2.KeyRequest(
            key=key, atTx=atTx, sinceTx=sinceTx, noWait=noWait, atRevision=atRevision), proveSinceTx=proveSinceTx)
        resp = self._stub.streamVerifiableGet(req._getGRPC())
        reader = VerifiedGetStreamReader(resp)
        chunks = reader.chunks()
        key = next(chunks, None)
        value = b''
        if key != None:
            verifiableTx = next(chunks)
            inclusionProof = next(chunks)
            for chunk in chunks:
                value += chunk.chunk
            verified = verifyTransaction(
                verifiableTx, state, self._vk, self._rs)
            if (len(verified) == 0):
                raise ErrCorruptedData
            return datatypes.SafeGetResponse(
                id=verifiableTx.tx.header.id,
                key=key.key,
                value=value,
                timestamp=verifiableTx.tx.header.ts,
                verified=True,
                refkey=key.refKey,
                revision=atRevision
            )

    def streamVerifiedGetBuffered(self, key: bytes = None, atTx: int = None, sinceTx: int = None, noWait: bool = None, atRevision: int = None) -> Tuple[datatypes.SafeGetResponse, BufferedStreamReader]:
        """Gets a value of a key with streaming method, and verifies transaction. Value is represented as BufferedStreamReader

        Args:
            key (bytes): Key to get
            atTx (int, optional): Get key at transaction id. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.
            atRevision (int, optional): Returns value of key at specified revision. -1 to get relative revision. Defaults to None.

        Raises:
            ErrCorruptedData: When data is corrupted or unverifiable

        Returns:
            Tuple[datatypes.SafeGetResponse, BufferedStreamReader]: First element is safe get response without value, second is a buffer that you can read from
        """
        state = self._rs.get()
        proveSinceTx = state.txId
        req = datatypesv2.VerifiableGetRequest(keyRequest=datatypesv2.KeyRequest(
            key=key, atTx=atTx, sinceTx=sinceTx, noWait=noWait, atRevision=atRevision), proveSinceTx=proveSinceTx)
        resp = self._stub.streamVerifiableGet(req._getGRPC())
        reader = VerifiedGetStreamReader(resp)
        chunks = reader.chunks()
        key = next(chunks, None)
        if key != None:
            verifiableTx = next(chunks)
            inclusionProof = next(chunks)
            verified = verifyTransaction(
                verifiableTx, state, self._vk, self._rs)
            if (len(verified) == 0):
                raise ErrCorruptedData
            toRet = datatypes.SafeGetResponse(
                id=verifiableTx.tx.header.id,
                key=key.key,
                value=None,
                timestamp=verifiableTx.tx.header.ts,
                verified=True,
                refkey=key.refKey,
                revision=atRevision
            )
            valueHeader = next(chunks)
            return toRet, BufferedStreamReader(chunks, valueHeader, resp)

    def streamHistory(self, key: bytes, offset: int = None, sinceTx: int = None, limit: int = None, desc: bool = None) -> Generator[datatypesv2.KeyValue, None, None]:
        """Streams history of key

        Args:
            key (bytes): Key to find
            offset (int, optional): Offset to apply
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.
            desc (bool, optional): Descending or ascending order. Defaults to None.

        Yields:
            Generator[datatypesv2.KeyValue, None, None]: Generator of KeyValues
        """
        request = datatypesv2.HistoryRequest(
            key=key, offset=offset, limit=limit, desc=desc, sinceTx=sinceTx)
        resp = self._stub.streamHistory(request._getGRPC())
        key = None
        value = None
        for chunk in StreamReader(resp).chunks():
            if isinstance(chunk, KeyHeader):
                if key != None:
                    yield datatypesv2.KeyValue(key=key, value=value, metadata=None)
                key = chunk.key
                value = b''
            else:
                value += chunk.chunk

        if key != None and value != None:  # situation when generator consumes all at first run, so it didn't yield first value
            yield datatypesv2.KeyValue(key=key, value=value, metadata=None)

    def streamHistoryBuffered(self, key: bytes, offset: int = None, sinceTx: int = None, limit: int = None, desc: bool = None) -> Generator[Tuple[datatypesv2.KeyValue, BufferedStreamReader], None, None]:
        """Streams history of key

        Args:
            key (bytes): Key to find
            offset (int, optional): Offset to apply
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): Doesn't wait for the index to be fully generated. Defaults to None.
            desc (bool, optional): Descending or ascending order. Defaults to None.

        Yields:
            Generator[Tuple[datatypesv2.KeyValue, BufferedStreamReader], None, None]: Generator of Tuples of KeyValue and BufferedStreamReader. You can read from BufferedStreamReader with read() method
        """
        request = datatypesv2.HistoryRequest(
            key=key, offset=offset, limit=limit, desc=desc, sinceTx=sinceTx)
        resp = self._stub.streamHistory(request._getGRPC())
        key = None
        valueHeader = None

        streamReader = StreamReader(resp)
        chunks = streamReader.chunks()
        chunk = next(chunks, None)
        while chunk != None:
            if isinstance(chunk, KeyHeader):
                key = chunk.key
                valueHeader = next(chunks)
                yield key, BufferedStreamReader(chunks, valueHeader, resp)
            chunk = next(chunks, None)

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
        yield Chunk(content=KeyHeader(key=key, length=len(key)).getInBytes())
        firstChunk = buffer.read(chunkSize)
        firstChunk = ValueChunkHeader(
            chunk=firstChunk, length=length).getInBytes()
        yield Chunk(content=firstChunk)
        chunk = buffer.read(chunkSize)
        while chunk:
            yield Chunk(content=chunk)
            chunk = buffer.read(chunkSize)

    def _make_verifiable_set_stream(self, buffer, key: bytes, length: int, provenSinceTx: int = None, chunkSize: int = 65536):
        """Helper function to create stream from provided buffer

        Args:
            buffer (io.BytesIO): Any buffer
            key (bytes): Key to set
            length (int): Length of buffer
            provenSinceTx (int): Prove since this transaction id
            chunkSize (int, optional): Chunk size. Defaults to 65536.

        Yields:
            Generator[Chunk, None, None]: Yields GRPC chunks
        """
        header = ProvenSinceHeader(provenSinceTx)
        yield Chunk(content=header.getInBytes())
        yield Chunk(content=KeyHeader(key=key, length=len(key)).getInBytes())
        firstChunk = buffer.read(chunkSize)
        firstChunk = ValueChunkHeader(
            chunk=firstChunk, length=length).getInBytes()
        yield Chunk(content=firstChunk)
        chunk = buffer.read(chunkSize)
        while chunk:
            yield Chunk(content=chunk)
            chunk = buffer.read(chunkSize)

    def streamZScanBuffered(self, set: bytes = None, seekKey: bytes = None,
                            seekScore: float = None, seekAtTx: int = None, inclusiveSeek: bool = None, limit: int = None,
                            desc: bool = None, minScore: float = None, maxScore: float = None, sinceTx: int = None, noWait: bool = None, offset: int = None) -> Generator[Tuple[datatypesv2.ZScanEntry, BufferedStreamReader], None, None]:
        """Scan for provided parameters for secondary index. Limit for scan is fixed - 1000. You need to introduce pagination.
        This method returns buffered ZEntry values - you need to read from value yourself by read(int) method.

        Args:
            set (bytes, optional): Set name. Defaults to None.
            seekKey (bytes, optional): Seek key to find. Defaults to None.
            seekScore (float, optional): Seek score to find. Defaults to None.
            seekAtTx (int, optional): TX id for the first entry. Defaults to None.
            inclusiveSeek (bool, optional): Element specified in seek should be included. Defaults to None.
            limit (int, optional): Maximum number of returned items. Defaults to None.
            desc (bool, optional): Descending or ascending order. Defaults to None.
            minScore (float, optional): Minimum score to find. Defaults to None.
            maxScore (float, optional): Maximum score to find. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): when true - scan doesn't wait for transaction at seekAtTx to be procesessed. Defaults to False.
            offset (int, optional): Offsets current scan. Defaults to None.

        Yields:
            Generator[Tuple[datatypesv2.ZScanEntry, BufferedStreamReader]]: Returns generator of Tuple of ZScanEntry and BufferedStreamReader. You can read from BufferedStreamReader with read(int) method
        """
        minScoreObject = None
        maxScoreObject = None
        if minScore != None:
            minScoreObject = datatypesv2.Score(minScore)
        if maxScore != None:
            maxScoreObject = datatypesv2.Score(maxScore)
        req = datatypesv2.ZScanRequest(set=set, seekKey=seekKey, seekScore=seekScore, seekAtTx=seekAtTx, inclusiveSeek=inclusiveSeek,
                                       limit=limit, desc=desc, minScore=minScoreObject, maxScore=maxScoreObject, sinceTx=sinceTx, noWait=noWait, offset=offset)
        resp = self._stub.streamZScan(req._getGRPC())

        set = None
        key = None
        score = None
        atTx = None

        chunks = ZScanStreamReader(resp).chunks()
        chunk = next(chunks, None)
        while chunk != None:
            if isinstance(chunk, SetHeader):
                set = chunk.set
                atTx = None
                score = None
                key = None

            elif isinstance(chunk, KeyHeader):
                key = chunk.key

            elif isinstance(chunk, ScoreHeader):
                score = chunk.score

            elif isinstance(chunk, AtTXHeader):
                atTx = chunk.seenAtTx

            else:
                yield datatypesv2.ZScanEntry(set=set, key=key, score=score, atTx=atTx), BufferedStreamReader(chunks, chunk, resp)

            chunk = next(chunks, None)

    def streamZScan(self, set: bytes = None, seekKey: bytes = None,
                    seekScore: float = None, seekAtTx: int = None, inclusiveSeek: bool = None, limit: int = None,
                    desc: bool = None, minScore: float = None, maxScore: float = None, sinceTx: int = None, noWait: bool = False, offset: int = None) -> Generator[datatypesv2.ZScanEntry, None, None]:
        """Scan for provided parameters for secondary index. Limit for scan is fixed - 1000. You need to introduce pagination.

        Args:
            set (bytes, optional): Set name. Defaults to None.
            seekKey (bytes, optional): Seek key to find. Defaults to None.
            seekScore (float, optional): Seek score to find. Defaults to None.
            seekAtTx (int, optional): TX id for the first entry. Defaults to None.
            inclusiveSeek (bool, optional): Element specified in seek should be included. Defaults to None.
            limit (int, optional): Maximum number of returned items. Defaults to None.
            desc (bool, optional): Descending or ascending order. Defaults to None.
            minScore (float, optional): Minimum score to find. Defaults to None.
            maxScore (float, optional): Maximum score to find. Defaults to None.
            sinceTx (int, optional): immudb will wait for transaction provided by sinceTx. Defaults to None.
            noWait (bool, optional): when true - scan doesn't wait for transaction at seekAtTx to be procesessed. Defaults to False.
            offset (int, optional): Offsets current scan. Defaults to None.

        Yields:
            Generator[datatypesv2.ZScanEntry, None, None]: Returns generator of ZScanEntry
        """
        minScoreObject = None
        maxScoreObject = None
        if minScore != None:
            minScoreObject = datatypesv2.Score(minScore)
        if maxScore != None:
            maxScoreObject = datatypesv2.Score(maxScore)
        req = datatypesv2.ZScanRequest(set=set, seekKey=seekKey, seekScore=seekScore, seekAtTx=seekAtTx, inclusiveSeek=inclusiveSeek,
                                       limit=limit, desc=desc, minScore=minScoreObject, maxScore=maxScoreObject, sinceTx=sinceTx, noWait=noWait, offset=offset)
        resp = self._stub.streamZScan(req._getGRPC())

        set = None
        key = None
        score = None
        atTx = None
        value = None
        for chunk in ZScanStreamReader(resp).chunks():
            if isinstance(chunk, SetHeader):
                if set != None:
                    yield datatypesv2.ZScanEntry(set=set, key=key, value=value, score=score, atTx=atTx)
                set = chunk.set
                value = b''
                atTx = None
                score = None
                key = None

            elif isinstance(chunk, KeyHeader):
                key = chunk.key

            elif isinstance(chunk, ScoreHeader):
                score = chunk.score

            elif isinstance(chunk, AtTXHeader):
                atTx = chunk.seenAtTx

            else:
                value += chunk.chunk

        if key != None and value != None:  # situation when generator consumes all at first run, so it didn't yield first value
            yield datatypesv2.ZScanEntry(set=set, key=key, value=value, score=score, atTx=atTx)

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
        req = datatypesv2.ScanRequest(seekKey=seekKey, endKey=endKey, prefix=prefix, desc=desc, limit=limit,
                                      sinceTx=sinceTx, noWait=noWait, inclusiveSeek=None, inclusiveEnd=None, offset=None)
        resp = self._stub.streamScan(req._getGRPC())
        key = None
        value = None
        for chunk in StreamReader(resp).chunks():
            if isinstance(chunk, KeyHeader):
                if key != None:
                    yield datatypesv2.KeyValue(key=key, value=value, metadata=None)
                key = chunk.key
                value = b''
            else:
                value += chunk.chunk

        if key != None and value != None:  # situation when generator consumes all at first run, so it didn't yield first value
            yield datatypesv2.KeyValue(key=key, value=value, metadata=None)

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

        req = datatypesv2.ScanRequest(seekKey=seekKey, endKey=endKey, prefix=prefix, desc=desc, limit=limit,
                                      sinceTx=sinceTx, noWait=noWait, inclusiveSeek=inclusiveSeek, inclusiveEnd=inclusiveEnd, offset=offset)
        resp = self._stub.streamScan(req._getGRPC())
        key = None
        valueHeader = None

        streamReader = StreamReader(resp)
        chunks = streamReader.chunks()
        chunk = next(chunks, None)
        while chunk != None:
            if isinstance(chunk, KeyHeader):
                key = chunk.key
                valueHeader = next(chunks)
                yield key, BufferedStreamReader(chunks, valueHeader, resp)
            chunk = next(chunks, None)

    def _rawStreamSet(self, generator: Generator[Chunk, None, None]) -> datatypesv2.TxHeader:
        """Helper function that grabs generator of chunks and set into opened stream

        Args:
            generator (Generator[Chunk, None, None]): Generator

        Returns:
            datatypesv2.TxHeader: Transaction header
        """
        resp = self._stub.streamSet(generator)
        return dataconverter.convertResponse(resp)

    def _raw_verifiable_stream_set(self, generator: Generator[Chunk, None, None]):
        """Helper function that grabs generator of chunks and set into opened stream

        Args:
            generator (Generator[Chunk, None, None]): Generator of chunks

        Returns:
            schema_pb2.VerifiableTx: Raw VerifiableTX object 
        """
        resp = self._stub.streamVerifiableSet(generator)
        return resp

    def _make_stream_exec_all_stream(self, ops: List[Union[datatypes.KeyValue, datatypes.StreamingKeyValue, datatypes.ZAddRequest, datatypes.ReferenceRequest]], noWait=False, chunkSize=65536) -> Generator[Chunk, None, None]:
        """Helper function that converts provided list into generator of Chunks

        Args:
            ops (List[Union[datatypes.KeyValue, datatypes.StreamingKeyValue, datatypes.ZAddRequest, datatypes.ReferenceRequest]]): List of actions to execute
            noWait (bool, optional): When true - scan doesn't wait for the index to be fully generated. Defaults to False.
            chunkSize (int, optional): Chunk size to set while streaming. Defaults to 65536.

        Yields:
            Generator[Chunk, None, None]: Generator of chunks
        """
        kv = 1
        zadd = 2
        for op in ops:
            if type(op) == datatypes.KeyValue:
                concated = int.to_bytes(1, 8, 'big')
                concated += int.to_bytes(kv, 1, 'big')
                yield Chunk(content=concated + KeyHeader(key=op.key, length=len(op.key)).getInBytes())
                buffer = BytesIO(op.value)
                firstChunk = buffer.read(chunkSize)
                firstChunk = ValueChunkHeader(
                    chunk=firstChunk, length=len(op.value)).getInBytes()
                yield Chunk(content=firstChunk)
                chunk = buffer.read(chunkSize)
                while chunk:
                    yield Chunk(content=chunk)
                    chunk = buffer.read(chunkSize)
            elif type(op) == datatypes.StreamingKeyValue:
                concated = int.to_bytes(1, 8, 'big')
                concated += int.to_bytes(kv, 1, 'big')
                yield Chunk(content=concated + KeyHeader(key=op.key, length=len(op.key)).getInBytes())
                buffer = op.value
                firstChunk = buffer.read(chunkSize)
                firstChunk = ValueChunkHeader(
                    chunk=firstChunk, length=op.length).getInBytes()
                yield Chunk(content=firstChunk)
                chunk = buffer.read(chunkSize)
                while chunk:
                    yield Chunk(content=chunk)
                    chunk = buffer.read(chunkSize)
            elif type(op) == datatypes.ZAddRequest:
                concated = int.to_bytes(1, 8, 'big')
                concated += int.to_bytes(zadd, 1, 'big')
                zAdd = schema_pb2.ZAddRequest(
                    set=op.set,
                    score=op.score,
                    key=op.key,
                    atTx=op.atTx,
                    boundRef=op.boundRef,
                    noWait=op.noWait
                )
                serialized = zAdd.SerializeToString()
                lengthOf = len(serialized)
                lengthBytes = int.to_bytes(lengthOf, 8, 'big')
                yield Chunk(content=concated + lengthBytes + serialized)

    def _raw_stream_exec_all(self, generator: Generator[Chunk, None, None]) -> datatypesv2.TxHeader:
        """Read everything from generator and yields into opened stream

        Args:
            generator (Generator[Chunk, None, None]): Chunk generator

        Returns:
            datatypesv2.TxHeader: TxHeader of just executed transaction
        """

        resp = dataconverter.convertResponse(
            self._stub.streamExecAll(generator))
        return resp

    def streamExecAll(self, ops: List[Union[datatypes.KeyValue, datatypes.StreamingKeyValue, datatypes.ZAddRequest, datatypes.ReferenceRequest]], noWait=False) -> datatypesv2.TxHeader:
        """Executes everything provided in ops List

        Args:
            ops (List[Union[datatypes.KeyValue, datatypes.StreamingKeyValue, datatypes.ZAddRequest, datatypes.ReferenceRequest]]): List of actions to execute
            noWait (bool, optional): When true - scan doesn't wait for the index to be fully generated. Defaults to False.

        Returns:
            TxHeader: TxHeader of just executed transaction
        """
        return self._raw_stream_exec_all(self._make_stream_exec_all_stream(ops, noWait))

    def streamVerifiedSet(self, key: bytes, buffer: BytesIO, bufferLength: int, chunkSize: int = 65536) -> datatypes.SetResponse:
        """Sets key into value with streaming method and verifies with current state

        Args:
            key (bytes): Key
            buffer (io.BytesIO): Any buffer that implements read(length: int) method
            bufferLength (int): Buffer length (protocol needs to know it at first)
            chunkSize (int, optional): Specifies chunk size while sending. Defaults to 65536. 

        Returns:
            datatypes.SetResponse: Response contains id of transaction and verification status.
            Raises exception if corrupted data.
        """
        state = self._rs.get()
        resp = self._raw_verifiable_stream_set(self._make_verifiable_set_stream(
            buffer, key, bufferLength, state.txId, chunkSize))
        verified = verifyTransaction(resp, state, self._vk, self._rs)

        return datatypes.SetResponse(
            id=resp.tx.header.id,
            verified=verified[0] == key,
        )

    def streamVerifiedSetFullValue(self, key: bytes, value: bytes, chunkSize: int = 65536) -> datatypes.SetResponse:
        """Sets key into value with streaming method and verifies with current state.

        Args:
            key (bytes): Key to set
            value (bytes): Value to set
            chunkSize (int, optional): Specifies chunk size while sending. Defaults to 65536.

        Returns:
            datatypes.SetResponse: Response contains id of transaction and verification status.
            Raises exception if corrupted data.
        """
        state = self._rs.get()
        resp = self._raw_verifiable_stream_set(self._make_verifiable_set_stream(
            BytesIO(value), key, len(value), state.txId, chunkSize))
        verified = verifyTransaction(resp, state, self._vk, self._rs)

        return datatypes.SetResponse(
            id=resp.tx.header.id,
            verified=verified[0] == key,
        )

    def streamSet(self, key: bytes, buffer, bufferLength: int, chunkSize: int = 65536) -> datatypesv2.TxHeader:
        """Sets key into value with streaming method.

        Args:
            key (bytes): Key
            buffer (io.BytesIO): Any buffer that implements read(length: int) method
            bufferLength (int): Buffer length (protocol needs to know it at first)
            chunkSize (int, optional): Specifies chunk size while sending. Defaults to 65536. 

        Returns:
            datatypesv2.TxHeader: Transaction header of just set transaction
        """
        resp = self._rawStreamSet(self._make_set_stream(
            buffer, key, bufferLength, chunkSize))
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
        resp = self._rawStreamSet(self._make_set_stream(
            BytesIO(value), key, len(value), chunkSize))
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

    def sqlQuery(self, query, params={}, columnNameMode=constants.COLUMN_NAME_MODE_NONE, acceptStream=False):
        """Queries the database using SQL
        Args:
            query: a query in immudb SQL dialect.
            params: a dictionary of parameters to replace in the query

        Returns:
            A list of table names. For example:

            ['table1', 'table2']
        """
        it = sqlquery.call(self._stub, self._rs, query,
                           params, columnNameMode, self._currentdb)
        if acceptStream:
            return it

        return list(it)

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

    def verifiableSQLGet(self, table: str, primaryKeys: List[datatypesv2.PrimaryKey], atTx=None, sinceTx=None) -> datatypesv2.VerifiableSQLEntry:
        """Verifies SQL row against current state

        Example:
            client.verifiableSQLGet(
                tabname, [datatypesv2.PrimaryKeyIntValue(1), datatypesv2.PrimaryKeyIntValue(3)]
            )

        Args:
            table (str): Table Name
            primaryKeys (List[datatypesv2.PrimaryKey]): List of PrimaryKeys to check
            atTx (int): Identifier of the transaction at which point the key's
                value should be retrieved.
            sinceTx (int): Identifier of the earliest transaction from which the
                key's value should be retrieved. If the specified transaction has
                not been indexed by immudb, this method will block until it has.

        Returns:
            datatypesv2.VerifiableSQLEntry: Contains all informations about just verified SQL Entry
        """
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
