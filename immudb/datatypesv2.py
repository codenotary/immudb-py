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
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional
from google.protobuf.struct_pb2 import NullValue
import immudb.grpc.schema_pb2 as schema

class GRPCTransformable:
    def getGRPC(self):
        transformed = self._transformDict(self.__dict__)
        schemaFrom = schema.__dict__.get(self.__class__.__name__, None)
        if(schemaFrom):
            return schemaFrom(**transformed)
        else: # Special case. Message could be nested inside Precondition schema
            schemaFrom = schema.__dict__["Precondition"].__dict__.get(self.__class__.__name__, None)
            if schemaFrom:
                return schemaFrom(**transformed)
            else:
                raise Exception("Cannot get schema for", self.__class__.__name__)

    def _transformDict(self, dictToTransform: Dict[str, Any]):
        for key in dictToTransform:
            currentValue = dictToTransform[key]
            if(isinstance(currentValue, GRPCTransformable)):
                dictToTransform[key] = currentValue.getGRPC()
            elif(isinstance(currentValue, list)):
                for index in range(0, len(currentValue)):
                    if(isinstance(currentValue[index], GRPCTransformable)):
                        currentValue[index] = currentValue[index].getGRPC()
            elif(isinstance(currentValue, Enum)):
                dictToTransform[key] = currentValue.value
        return dictToTransform




@dataclass
class Key(GRPCTransformable):
    key: bytes

@dataclass
class Permission(GRPCTransformable):
    database: str
    permission: int

@dataclass
class User(GRPCTransformable):
    user: bytes
    permissions: List[Permission]
    createdby: str
    createdat: str
    active: bool

@dataclass
class UserList(GRPCTransformable):
    users: List[User]

@dataclass
class CreateUserRequest(GRPCTransformable):
    user: bytes
    password: bytes
    permission: int
    database: str

@dataclass
class UserRequest(GRPCTransformable):
    user: bytes

@dataclass
class ChangePasswordRequest(GRPCTransformable):
    user: bytes
    oldPassword: bytes
    newPassword: bytes

@dataclass
class LoginRequest(GRPCTransformable):
    user: bytes
    password: bytes

@dataclass
class LoginResponse(GRPCTransformable):
    token: str
    warning: bytes

@dataclass
class AuthConfig(GRPCTransformable):
    kind: int

@dataclass
class MTLSConfig(GRPCTransformable):
    enabled: bool

@dataclass
class OpenSessionRequest(GRPCTransformable):
    username: bytes
    password: bytes
    databaseName: str

@dataclass
class OpenSessionResponse(GRPCTransformable):
    sessionID: str
    serverUUID: str

# ONE OF
@dataclass
class Precondition(GRPCTransformable):
    keyMustExist: Optional[KeyMustExistPrecondition] = None
    keyMustNotExist: Optional[KeyMustNotExistPrecondition] = None
    keyNotModifiedAfterTX: Optional[KeyNotModifiedAfterTXPrecondition] = None

@dataclass
class KeyMustExistPrecondition(GRPCTransformable):
    key: bytes

@dataclass
class KeyMustNotExistPrecondition(GRPCTransformable):
    key: bytes
    
class KeyNotModifiedAfterTXPrecondition(GRPCTransformable):
    key: bytes
    txID: int

@dataclass
class KeyValue(GRPCTransformable):
    key: bytes
    value: bytes
    metadata: KVMetadata

@dataclass
class Entry(GRPCTransformable):
    tx: int
    key: bytes
    value: bytes
    referencedBy: Reference
    metadata: KVMetadata
    expired: bool
    revision: int

@dataclass
class Reference(GRPCTransformable):
    tx: int
    key: bytes
    atTx: int
    metadata: KVMetadata
    revision: int

# ONE OF
@dataclass
class Op(GRPCTransformable):
    kv: Optional[KeyValue] = None
    zAdd: Optional[ZAddRequest] = None
    ref: Optional[ReferenceRequest] = None

@dataclass
class ExecAllRequest(GRPCTransformable):
    Operations: List[Op]
    noWait: bool
    preconditions: List[Precondition]

@dataclass
class Entries(GRPCTransformable):
    entries: List[Entry]

@dataclass
class ZEntry(GRPCTransformable):
    set: bytes
    key: bytes
    entry: Entry
    score: float
    atTx: int

@dataclass
class ZEntries(GRPCTransformable):
    entries: ZEntry

@dataclass
class ScanRequest(GRPCTransformable):
    seekKey: bytes
    endKey: bytes
    prefix: bytes
    desc: bool
    limit: int
    sinceTx: int
    noWait: bool
    inclusiveSeek: bool
    inclusiveEnd: bool
    offset: int

@dataclass
class KeyPrefix(GRPCTransformable):
    prefix: bytes

@dataclass
class EntryCount(GRPCTransformable):
    count: int

@dataclass
class Signature(GRPCTransformable):
    publicKey: bytes
    signature: bytes

@dataclass
class TxHeader(GRPCTransformable):
    id: int = None
    prevAlh: bytes = None
    ts: int = None
    nentries: int = None
    eH: bytes = None
    blTxId: int = None
    blRoot: bytes = None
    version: int = None
    metadata: TxMetadata = None
    

@dataclass
class TxMetadata(GRPCTransformable):
    pass

@dataclass
class LinearProof(GRPCTransformable):
    sourceTxId: int
    TargetTxId: int
    terms: List[bytes]

@dataclass
class DualProof(GRPCTransformable):
    sourceTxHeader: TxHeader
    targetTxHeader: TxHeader
    inclusionProof: List[bytes] = None
    consistencyProof: List[bytes] = None
    targetBlTxAlh: bytes = None
    lastInclusionProof: List[bytes] = None
    linearProof: LinearProof = None

@dataclass
class Tx(GRPCTransformable):
    header: TxHeader
    entries: List[TxEntry] = None
    kvEntries:  List[Entry] = None
    zEntries:  List[ZEntry] = None

@dataclass
class TxEntry(GRPCTransformable):
    key: bytes
    hValue: bytes
    vLen: int
    metadata: KVMetadata
    value: bytes

@dataclass
class KVMetadata(GRPCTransformable):
    deleted: bool
    expiration: Expiration
    nonIndexable: bool

@dataclass
class Expiration(GRPCTransformable):
    expiresAt: int

@dataclass
class VerifiableTx(GRPCTransformable):
    tx: Tx
    dualProof: DualProof
    signature: Signature

@dataclass
class VerifiableEntry(GRPCTransformable):
    entry: Entry = None
    verifiableTx: VerifiableTx = None
    inclusionProof: InclusionProof = None

@dataclass
class InclusionProof(GRPCTransformable):
    leaf: int
    width: int
    terms: List[bytes]

@dataclass
class SetRequest(GRPCTransformable):
    KVs: List[KeyValue]
    noWait: bool
    preconditions: List[Precondition]

@dataclass
class KeyRequest(GRPCTransformable):
    key: bytes
    atTx: int
    sinceTx: int
    noWait: bool
    atRevision: int

@dataclass
class KeyListRequest(GRPCTransformable):
    keys: List[bytes]
    sinceTx: int

@dataclass
class DeleteKeysRequest(GRPCTransformable):
    keys: List[bytes]
    sinceTx: int
    noWait: bool

@dataclass
class VerifiableSetRequest(GRPCTransformable):
    setRequest: SetRequest
    proveSinceTx: int

@dataclass
class VerifiableGetRequest(GRPCTransformable):
    keyRequest: KeyRequest
    proveSinceTx: int

@dataclass
class ServerInfoRequest(GRPCTransformable):
    pass

@dataclass
class ServerInfoResponse(GRPCTransformable):
    version: str

@dataclass
class HealthResponse(GRPCTransformable):
    status: bool
    version: str

@dataclass
class DatabaseHealthResponse(GRPCTransformable):
    pendingRequests: int
    lastRequestCompletedAt: int

@dataclass
class ImmutableState(GRPCTransformable):
    db: str
    txId: int
    txHash: bytes
    signature: Signature

@dataclass
class ReferenceRequest(GRPCTransformable):
    key: bytes
    referencedKey: bytes
    atTx: int
    boundRef: bool
    noWait: bool
    preconditions: List[Precondition]

@dataclass
class VerifiableReferenceRequest(GRPCTransformable):
    referenceRequest: ReferenceRequest
    proveSinceTx: int

@dataclass
class ZAddRequest(GRPCTransformable):
    set: bytes
    score: float
    key: bytes
    atTx: int
    boundRef: bool
    noWait: bool

@dataclass
class Score(GRPCTransformable):
    score: float

@dataclass
class ZScanRequest(GRPCTransformable):
    set: bytes
    seekKey: bytes
    seekScore: float
    seekAtTx: int
    inclusiveSeek: bool
    limit: int
    desc: bool
    minScore: Score
    maxScore: Score
    sinceTx: int
    noWait: bool
    offset: int

@dataclass
class HistoryRequest(GRPCTransformable):
    key: bytes
    offset: int
    limit: int
    desc: bool
    sinceTx: int


@dataclass
class VerifiableZAddRequest(GRPCTransformable):
    zAddRequest: ZAddRequest
    proveSinceTx: int

@dataclass
class TxRequest(GRPCTransformable):
    tx: int
    entriesSpec: EntriesSpec
    sinceTx: int
    noWait: bool
    keepReferencesUnresolved: bool

@dataclass
class EntriesSpec(GRPCTransformable):
    kvEntriesSpec: EntryTypeSpec = None
    zEntriesSpec: EntryTypeSpec = None
    sqlEntriesSpec: EntryTypeSpec = None


@dataclass
class EntryTypeSpec(GRPCTransformable):
    action: EntryTypeAction

class EntryTypeAction(Enum):
    EXCLUDE = 0
    ONLY_DIGEST = 1
    RAW_VALUE = 2
    RESOLVE = 3

@dataclass
class VerifiableTxRequest(GRPCTransformable):
    tx: int
    proveSinceTx: int
    entriesSpec: EntriesSpec
    sinceTx: int
    noWait: bool
    keepReferencesUnresolved: bool

@dataclass
class TxScanRequest(GRPCTransformable):
    initialTx: int = None
    limit: int = None
    desc: bool = None
    entriesSpec: EntriesSpec = None
    sinceTx: int = None
    noWait: bool = None

@dataclass
class TxList(GRPCTransformable):
    txs: List[Tx]

@dataclass
class ExportTxRequest(GRPCTransformable):
    tx: int 

@dataclass
class Database(GRPCTransformable):
    databaseName: str

@dataclass
class DatabaseSettings(GRPCTransformable):
    databaseName: str
    replica: bool 
    masterDatabase: str
    masterAddress: str
    masterPort: int 
    followerUsername: str 
    followerPassword: str 
    fileSize: int
    maxKeyLen: int 
    maxValueLen: int 
    maxTxEntries: int 
    excludeCommitTime: bool 

@dataclass
class CreateDatabaseRequest(GRPCTransformable):
    name: str
    settings: DatabaseNullableSettings
    ifNotExists: bool


@dataclass
class CreateDatabaseResponse(GRPCTransformable):
    name: str
    settings: DatabaseNullableSettings
    alreadyExisted: bool

@dataclass
class UpdateDatabaseRequest(GRPCTransformable):
    database: str
    settings: DatabaseNullableSettings

@dataclass
class UpdateDatabaseResponse(GRPCTransformable):
    database: str
    settings: DatabaseNullableSettings


@dataclass
class DatabaseSettingsRequest(GRPCTransformable):
    pass

@dataclass
class DatabaseSettingsResponse(GRPCTransformable):
    database: str
    settings: DatabaseNullableSettings

@dataclass
class NullableUint32(GRPCTransformable):
    value: int

@dataclass
class NullableUint64(GRPCTransformable):
    value: int

@dataclass
class NullableFloat(GRPCTransformable):
    value: float

@dataclass
class NullableBool(GRPCTransformable):
    value: bool

@dataclass
class NullableString(GRPCTransformable):
    value: str

@dataclass
class NullableMilliseconds(GRPCTransformable):
    value: int

@dataclass
class DatabaseNullableSettings(GRPCTransformable):
    replicationSettings:  ReplicationNullableSettings 
    fileSize:  NullableUint32 
    maxKeyLen:  NullableUint32 
    maxValueLen:  NullableUint32 
    maxTxEntries:  NullableUint32 
    excludeCommitTime:  NullableBool 
    maxConcurrency:  NullableUint32 
    maxIOConcurrency:  NullableUint32 
    txLogCacheSize:  NullableUint32 
    vLogMaxOpenedFiles:  NullableUint32 
    txLogMaxOpenedFiles:  NullableUint32 
    commitLogMaxOpenedFiles:  NullableUint32 
    indexSettings:  IndexNullableSettings 
    writeTxHeaderVersion:  NullableUint32 
    autoload:  NullableBool 
    readTxPoolSize:  NullableUint32 
    syncFrequency:  NullableMilliseconds 
    writeBufferSize:  NullableUint32 
    ahtSettings:  AHTNullableSettings 

@dataclass
class ReplicationNullableSettings(GRPCTransformable):
    replica:  NullableBool 
    masterDatabase:  NullableString 
    masterAddress:  NullableString 
    masterPort:  NullableUint32 
    followerUsername:  NullableString 
    followerPassword:  NullableString
    

@dataclass
class IndexNullableSettings(GRPCTransformable):
    flushThreshold:  NullableUint32 
    syncThreshold:  NullableUint32 
    cacheSize:  NullableUint32 
    maxNodeSize:  NullableUint32 
    maxActiveSnapshots:  NullableUint32 
    renewSnapRootAfter:  NullableUint64 
    compactionThld:  NullableUint32 
    delayDuringCompaction:  NullableUint32 
    nodesLogMaxOpenedFiles:  NullableUint32 
    historyLogMaxOpenedFiles:  NullableUint32 
    commitLogMaxOpenedFiles:  NullableUint32 
    flushBufferSize:  NullableUint32 
    cleanupPercentage:  NullableFloat

@dataclass
class AHTNullableSettings(GRPCTransformable):
    syncThreshold: NullableUint32
    writeBufferSize: NullableUint32

@dataclass
class LoadDatabaseRequest(GRPCTransformable):
    database: str

@dataclass
class LoadDatabaseResponse(GRPCTransformable):
    database: str

@dataclass
class UnloadDatabaseRequest(GRPCTransformable):
    database: str

@dataclass
class UnloadDatabaseResponse(GRPCTransformable):
    database: str

@dataclass
class DeleteDatabaseRequest(GRPCTransformable):
    database: str

@dataclass
class DeleteDatabaseResponse(GRPCTransformable):
    database: str

@dataclass
class FlushIndexRequest(GRPCTransformable):
    cleanupPercentage: float
    synced: bool

@dataclass
class FlushIndexResponse(GRPCTransformable):
    database: str

@dataclass
class Table(GRPCTransformable):
    tableName: str

@dataclass
class SQLGetRequest(GRPCTransformable):
    table: str
    pkValues: List[SQLValue] 
    atTx: int 
    sinceTx: int

@dataclass
class VerifiableSQLGetRequest(GRPCTransformable):
    sqlGetRequest: SQLGetRequest
    proveSinceTx: int

@dataclass
class SQLEntry(GRPCTransformable):
    tx: int
    key: bytes
    value: bytes
    metadata: KVMetadata

@dataclass
class VerifiableSQLEntry(GRPCTransformable):
    sqlEntry: SQLEntry = None
    verifiableTx: VerifiableTx = None
    inclusionProof: InclusionProof = None
    DatabaseId: int = None
    TableId: int = None
    PKIDs: List[int] = None
    ColNamesById: Dict[int, str] = None
    ColIdsByName: Dict[str, int] = None
    ColTypesById: Dict[int, str] = None
    ColLenById: Dict[int, int] = None

@dataclass
class UseDatabaseReply(GRPCTransformable):
    token: str

class PermissionAction(Enum):
    GRANT = 0
    REVOK = 1

@dataclass
class ChangePermissionRequest(GRPCTransformable):
    action: PermissionAction
    username: str
    database: str
    permission: int

@dataclass
class SetActiveUserRequest(GRPCTransformable):
    active: bool
    username: str

@dataclass
class DatabaseListResponse(GRPCTransformable):
    databases: List[Database]

@dataclass
class DatabaseListRequestV2(GRPCTransformable):
    pass

@dataclass
class DatabaseListResponseV2(GRPCTransformable):
    databases: List[DatabaseWithSettings]

@dataclass
class DatabaseWithSettings(GRPCTransformable):
    name: str
    setting: DatabaseNullableSettings
    loaded: bool

@dataclass
class Chunk(GRPCTransformable):
    content: bytes

@dataclass
class UseSnapshotRequest(GRPCTransformable):
    sinceTx: int
    asBeforeTx: int

@dataclass
class SQLExecRequest(GRPCTransformable):
    sql: str
    params: List[NamedParam]
    noWait: bool

@dataclass
class SQLQueryRequest(GRPCTransformable):
    sql: str
    params: List[NamedParam]
    reuseSnapshot: int

@dataclass
class NamedParam(GRPCTransformable):
    name: str
    value: SQLValue

@dataclass
class SQLExecResult(GRPCTransformable):
    txs: List[CommittedSQLTx]
    ongoingTx: bool

@dataclass
class CommittedSQLTx(GRPCTransformable):
    header: TxHeader
    updatedRows: int
    lastInsertedPKs: Dict[str, SQLValue]
    firstInsertedPKs: Dict[str, SQLValue]


@dataclass
class SQLQueryResult(GRPCTransformable):
    columns: List[Column]
    rows: List[Row]

@dataclass
class Column(GRPCTransformable):
    name: str
    type: str

@dataclass
class Row(GRPCTransformable):
    columns: List[str]
    values: List[SQLValue]

# ONE OF
@dataclass
class SQLValue(GRPCTransformable):
    null: Optional[NullValue] = None
    n: Optional[int] = None
    s: Optional[str] = None
    b: Optional[bool] = None
    bs: Optional[bytes] = None
    ts: Optional[int] = None


class TxMode(Enum):
    ReadOnly = 0
    WriteOnly = 1
    ReadWrite = 2

@dataclass
class NewTxRequest(GRPCTransformable):
    mode: TxMode


@dataclass
class NewTxResponse(GRPCTransformable):
    transactionID : str

@dataclass
class ErrorInfo(GRPCTransformable):
    code: str
    cause: str

@dataclass
class DebugInfo(GRPCTransformable):
    stack: str

@dataclass
class RetryInfo(GRPCTransformable):
    retry_delay: int