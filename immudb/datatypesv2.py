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
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from google.protobuf.struct_pb2 import NullValue
import immudb.grpc.schema_pb2 as schema


def grpcHumanizator(objectFrom, classTo):
    finalKWArgs = dict()
    for key in objectFrom.__dict__.keys():
        if (objectFrom.__dict__[key] != None and isinstance(objectFrom.__dict__[key], GRPCTransformable)):
            finalKWArgs[key] = objectFrom.__dict__[key]._getHumanDataClass()
        elif (objectFrom.__dict__[key] != None):
            finalKWArgs[key] = objectFrom.__dict__[key]
        else:
            finalKWArgs[key] = None
    return classTo(**finalKWArgs)


class GRPCTransformable:
    def _getGRPC(self):
        transformed = self._transformDict(self.__dict__)
        schemaFrom = schema.__dict__.get(self.__class__.__name__, None)
        if (schemaFrom):
            return schemaFrom(**transformed)
        else:  # Special case. Message could be nested inside Precondition schema
            schemaFrom = schema.__dict__["Precondition"].__dict__.get(
                self.__class__.__name__, None)
            if schemaFrom:
                return schemaFrom(**transformed)
            else:
                raise Exception("Cannot get schema for",
                                self.__class__.__name__)

    def _transformDict(self, dictToTransform: Dict[str, Any]):
        for key in dictToTransform:
            currentValue = dictToTransform[key]
            if (isinstance(currentValue, GRPCTransformable)):
                dictToTransform[key] = currentValue._getGRPC()
            elif (isinstance(currentValue, list)):
                for index in range(0, len(currentValue)):
                    if (isinstance(currentValue[index], GRPCTransformable)):
                        currentValue[index] = currentValue[index]._getGRPC()
            elif (isinstance(currentValue, Enum)):
                dictToTransform[key] = currentValue.value
        return dictToTransform

    def _getHumanDataClass(self):
        return self


@dataclass
class Key(GRPCTransformable):
    key: bytes = None


@dataclass
class Permission(GRPCTransformable):
    database: str = None
    permission: int = None


@dataclass
class User(GRPCTransformable):
    user: bytes = None
    permissions: List[Permission] = None
    createdby: str = None
    createdat: str = None
    active: bool = None


@dataclass
class UserList(GRPCTransformable):
    users: List[User] = None


@dataclass
class CreateUserRequest(GRPCTransformable):
    user: bytes = None
    password: bytes = None
    permission: int = None
    database: str = None


@dataclass
class UserRequest(GRPCTransformable):
    user: bytes = None


@dataclass
class ChangePasswordRequest(GRPCTransformable):
    user: bytes = None
    oldPassword: bytes = None
    newPassword: bytes = None


@dataclass
class LoginRequest(GRPCTransformable):
    user: bytes = None
    password: bytes = None


@dataclass
class LoginResponse(GRPCTransformable):
    token: str = None
    warning: bytes = None


@dataclass
class AuthConfig(GRPCTransformable):
    kind: int = None


@dataclass
class MTLSConfig(GRPCTransformable):
    enabled: bool = None


@dataclass
class OpenSessionRequest(GRPCTransformable):
    username: bytes = None
    password: bytes = None
    databaseName: str = None


@dataclass
class OpenSessionResponse(GRPCTransformable):
    sessionID: str = None
    serverUUID: str = None

# ONE OF


@dataclass
class Precondition(GRPCTransformable):
    keyMustExist: Optional[KeyMustExistPrecondition] = None
    keyMustNotExist: Optional[KeyMustNotExistPrecondition] = None
    keyNotModifiedAfterTX: Optional[KeyNotModifiedAfterTXPrecondition] = None


@dataclass
class KeyMustExistPrecondition(GRPCTransformable):
    key: bytes = None


@dataclass
class KeyMustNotExistPrecondition(GRPCTransformable):
    key: bytes = None


class KeyNotModifiedAfterTXPrecondition(GRPCTransformable):
    key: bytes = None
    txID: int = None


@dataclass
class KeyValue(GRPCTransformable):
    key: bytes = None
    value: bytes = None
    metadata: KVMetadata = None


@dataclass
class Entry(GRPCTransformable):
    tx: int = None
    key: bytes = None
    value: bytes = None
    referencedBy: Reference = None
    metadata: KVMetadata = None
    expired: bool = None
    revision: int = None


@dataclass
class Reference(GRPCTransformable):
    tx: int = None
    key: bytes = None
    atTx: int = None
    metadata: KVMetadata = None
    revision: int = None

# ONE OF


@dataclass
class Op(GRPCTransformable):
    kv: Optional[KeyValue] = None
    zAdd: Optional[ZAddRequest] = None
    ref: Optional[ReferenceRequest] = None


@dataclass
class ExecAllRequest(GRPCTransformable):
    Operations: List[Op] = None
    noWait: bool = None
    preconditions: List[Precondition] = None


@dataclass
class Entries(GRPCTransformable):
    entries: List[Entry] = None


@dataclass
class ZEntry(GRPCTransformable):
    set: bytes = None
    key: bytes = None
    entry: Entry = None
    score: float = None
    atTx: int = None


@dataclass
class ZScanEntry():
    set: bytes = None
    key: bytes = None
    value: bytes = None
    score: float = None
    atTx: int = None


@dataclass
class ZEntries(GRPCTransformable):
    entries: ZEntry = None


@dataclass
class ScanRequest(GRPCTransformable):
    seekKey: bytes
    endKey: bytes = None
    prefix: bytes = None
    desc: bool = None
    limit: int = None
    sinceTx: int = None
    noWait: bool = None
    inclusiveSeek: bool = None
    inclusiveEnd: bool = None
    offset: int = None


@dataclass
class KeyPrefix(GRPCTransformable):
    prefix: bytes = None


@dataclass
class EntryCount(GRPCTransformable):
    count: int = None


@dataclass
class Signature(GRPCTransformable):
    publicKey: bytes = None
    signature: bytes = None


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
    sourceTxId: int = None
    TargetTxId: int = None
    terms: List[bytes] = None


@dataclass
class DualProof(GRPCTransformable):
    sourceTxHeader: TxHeader = None
    targetTxHeader: TxHeader = None
    inclusionProof: List[bytes] = None
    consistencyProof: List[bytes] = None
    targetBlTxAlh: bytes = None
    lastInclusionProof: List[bytes] = None
    linearProof: LinearProof = None


@dataclass
class Tx(GRPCTransformable):
    header: TxHeader = None
    entries: List[TxEntry] = None
    kvEntries:  List[Entry] = None
    zEntries:  List[ZEntry] = None


@dataclass
class TxEntry(GRPCTransformable):
    key: bytes = None
    hValue: bytes = None
    vLen: int = None
    metadata: KVMetadata = None
    value: bytes = None


@dataclass
class KVMetadata(GRPCTransformable):
    deleted: bool = None
    expiration: Expiration = None
    nonIndexable: bool = None


@dataclass
class Expiration(GRPCTransformable):
    expiresAt: int = None


@dataclass
class VerifiableTx(GRPCTransformable):
    tx: Tx = None
    dualProof: DualProof = None
    signature: Signature = None


@dataclass
class VerifiableEntry(GRPCTransformable):
    entry: Entry = None
    verifiableTx: VerifiableTx = None
    inclusionProof: InclusionProof = None


@dataclass
class InclusionProof(GRPCTransformable):
    leaf: int = None
    width: int = None
    terms: List[bytes] = None


@dataclass
class SetRequest(GRPCTransformable):
    KVs: List[KeyValue] = None
    noWait: bool = None
    preconditions: List[Precondition] = None


@dataclass
class KeyRequest(GRPCTransformable):
    key: bytes = None
    atTx: int = None
    sinceTx: int = None
    noWait: bool = None
    atRevision: int = None


@dataclass
class KeyListRequest(GRPCTransformable):
    keys: List[bytes] = None
    sinceTx: int = None


@dataclass
class DeleteKeysRequest(GRPCTransformable):
    keys: List[bytes] = None
    sinceTx: int = None
    noWait: bool = None


@dataclass
class VerifiableSetRequest(GRPCTransformable):
    setRequest: SetRequest = None
    proveSinceTx: int = None


@dataclass
class VerifiableGetRequest(GRPCTransformable):
    keyRequest: KeyRequest = None
    proveSinceTx: int = None


@dataclass
class ServerInfoRequest(GRPCTransformable):
    pass


@dataclass
class ServerInfoResponse(GRPCTransformable):
    version: str = None
    startedAt: int = None
    numTransactions: int = None
    numDatabases: int = None
    databasesDiskSize: int = None


@dataclass
class HealthResponse(GRPCTransformable):
    status: bool = None
    version: str = None


@dataclass
class DatabaseHealthResponse(GRPCTransformable):
    pendingRequests: int = None
    lastRequestCompletedAt: int = None


@dataclass
class ImmutableState(GRPCTransformable):
    db: str = None
    txId: int = None
    txHash: bytes = None
    signature: Signature = None


@dataclass
class ReferenceRequest(GRPCTransformable):
    key: bytes = None
    referencedKey: bytes = None
    atTx: int = None
    boundRef: bool = None
    noWait: bool = None
    preconditions: List[Precondition] = None


@dataclass
class VerifiableReferenceRequest(GRPCTransformable):
    referenceRequest: ReferenceRequest = None
    proveSinceTx: int = None


@dataclass
class ZAddRequest(GRPCTransformable):
    set: bytes = None
    score: float = None
    key: bytes = None
    atTx: int = None
    boundRef: bool = None
    noWait: bool = None


@dataclass
class Score(GRPCTransformable):
    score: float = None


@dataclass
class ZScanRequest(GRPCTransformable):
    set: bytes = None
    seekKey: bytes = None
    seekScore: float = None
    seekAtTx: int = None
    inclusiveSeek: bool = None
    limit: int = None
    desc: bool = None
    minScore: Score = None
    maxScore: Score = None
    sinceTx: int = None
    noWait: bool = None
    offset: int = None


@dataclass
class HistoryRequest(GRPCTransformable):
    key: bytes = None
    offset: int = None
    limit: int = None
    desc: bool = None
    sinceTx: int = None


@dataclass
class VerifiableZAddRequest(GRPCTransformable):
    zAddRequest: ZAddRequest = None
    proveSinceTx: int = None


@dataclass
class TxRequest(GRPCTransformable):
    tx: int = None
    entriesSpec: EntriesSpec = None
    sinceTx: int = None
    noWait: bool = None
    keepReferencesUnresolved: bool = None


@dataclass
class EntriesSpec(GRPCTransformable):
    kvEntriesSpec: EntryTypeSpec = None
    zEntriesSpec: EntryTypeSpec = None
    sqlEntriesSpec: EntryTypeSpec = None


@dataclass
class EntryTypeSpec(GRPCTransformable):
    action: EntryTypeAction = None


class EntryTypeAction(Enum):
    EXCLUDE = 0
    ONLY_DIGEST = 1
    RAW_VALUE = 2
    RESOLVE = 3


@dataclass
class VerifiableTxRequest(GRPCTransformable):
    tx: int = None
    proveSinceTx: int = None
    entriesSpec: EntriesSpec = None
    sinceTx: int = None
    noWait: bool = None
    keepReferencesUnresolved: bool = None


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
    txs: List[Tx] = None


@dataclass
class ExportTxRequest(GRPCTransformable):
    tx: int = None


@dataclass
class Database(GRPCTransformable):
    databaseName: str = None


@dataclass
class DatabaseSettings(GRPCTransformable):
    databaseName: str = None
    replica: bool = None
    masterDatabase: str = None
    masterAddress: str = None
    masterPort: int = None
    followerUsername: str = None
    followerPassword: str = None
    fileSize: int = None
    maxKeyLen: int = None
    maxValueLen: int = None
    maxTxEntries: int = None
    excludeCommitTime: bool = None


@dataclass
class CreateDatabaseRequest(GRPCTransformable):
    name: str = None
    settings: Union[DatabaseNullableSettings, DatabaseSettingsV2] = None
    ifNotExists: bool = None


@dataclass
class CreateDatabaseResponse(GRPCTransformable):
    name: str = None
    settings: DatabaseNullableSettings = None
    alreadyExisted: bool = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, CreateDatabaseResponseV2)


@dataclass
class CreateDatabaseResponseV2(GRPCTransformable):
    name: str = None
    settings: DatabaseSettingsV2 = None
    alreadyExisted: bool = None


@dataclass
class UpdateDatabaseRequest(GRPCTransformable):
    database: str = None
    settings: Union[DatabaseNullableSettings, DatabaseSettingsV2] = None


@dataclass
class UpdateDatabaseResponse(GRPCTransformable):
    database: str = None
    settings: DatabaseNullableSettings = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, UpdateDatabaseResponseV2)


@dataclass
class UpdateDatabaseResponseV2(GRPCTransformable):
    database: str = None
    settings: DatabaseSettingsV2 = None


@dataclass
class DatabaseSettingsRequest(GRPCTransformable):
    pass


@dataclass
class DatabaseSettingsResponse(GRPCTransformable):
    database: str = None
    settings: DatabaseNullableSettings = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, DatabaseSettingsResponseV2)


@dataclass
class DatabaseSettingsResponseV2(GRPCTransformable):
    database: str = None
    settings: DatabaseSettingsV2 = None


@dataclass
class NullableUint32(GRPCTransformable):
    value: int = None

    def _getGRPC(self):
        if self.value == None:
            return None
        else:
            return schema.NullableUint32(value=self.value)

    def _getHumanDataClass(self):
        return self.value


@dataclass
class NullableUint64(GRPCTransformable):
    value: int = None

    def _getGRPC(self):
        if self.value == None:
            return None
        else:
            return schema.NullableUint64(value=self.value)

    def _getHumanDataClass(self):
        return self.value


@dataclass
class NullableFloat(GRPCTransformable):
    value: float = None

    def _getGRPC(self):
        if self.value == None:
            return None
        else:
            return schema.NullableFloat(value=self.value)

    def _getHumanDataClass(self):
        return self.value


@dataclass
class NullableBool(GRPCTransformable):
    value: bool = None

    def _getGRPC(self):

        if self.value == None:
            return None
        else:
            return schema.NullableBool(value=self.value)

    def _getHumanDataClass(self):
        if self.value == None:
            return False
        return self.value


@dataclass
class NullableString(GRPCTransformable):
    value: str = None

    def _getGRPC(self):
        if self.value == None:
            return None
        else:
            return schema.NullableString(value=self.value)

    def _getHumanDataClass(self):
        return self.value


@dataclass
class NullableMilliseconds(GRPCTransformable):
    value: int = None

    def _getGRPC(self):
        if self.value == None:
            return None
        else:
            return schema.NullableMilliseconds(value=self.value)

    def _getHumanDataClass(self):
        return self.value


@dataclass
class DatabaseNullableSettings(GRPCTransformable):
    replicationSettings:  ReplicationNullableSettings = None
    fileSize:  NullableUint32 = None
    maxKeyLen:  NullableUint32 = None
    maxValueLen:  NullableUint32 = None
    maxTxEntries:  NullableUint32 = None
    excludeCommitTime:  NullableBool = None
    maxConcurrency:  NullableUint32 = None
    maxIOConcurrency:  NullableUint32 = None
    txLogCacheSize:  NullableUint32 = None
    vLogMaxOpenedFiles:  NullableUint32 = None
    txLogMaxOpenedFiles:  NullableUint32 = None
    commitLogMaxOpenedFiles:  NullableUint32 = None
    indexSettings:  IndexNullableSettings = None
    writeTxHeaderVersion:  NullableUint32 = None
    autoload:  NullableBool = None
    readTxPoolSize:  NullableUint32 = None
    syncFrequency:  NullableMilliseconds = None
    writeBufferSize:  NullableUint32 = None
    ahtSettings:  AHTNullableSettings = None
    maxActiveTransactions: NullableUint32 = None
    mvccReadSetLimit: NullableUint32 = None
    vLogCacheSize: NullableUint32 = None
    truncationSettings: TruncationNullableSettings = None
    embeddedValues: NullableBool = None
    preallocFiles: NullableBool = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, DatabaseSettingsV2)


@dataclass
class TruncationNullableSettings(GRPCTransformable):
    retentionPeriod: NullableMilliseconds = None
    truncationFrequency: NullableMilliseconds = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, TruncationSettings)


@dataclass
class ReplicationSettings(GRPCTransformable):
    replica:  Optional[bool] = None
    primaryDatabase:  Optional[str] = None
    primaryHost:  Optional[str] = None
    primaryPort:  Optional[int] = None
    primaryUsername:  Optional[str] = None
    primaryPassword:  Optional[str] = None
    syncReplication: Optional[bool] = None
    syncAcks: Optional[int] = None
    prefetchTxBufferSize: Optional[int] = None
    replicationCommitConcurrency: Optional[int] = None
    allowTxDiscarding: Optional[bool] = None
    skipIntegrityCheck: Optional[bool] = None
    waitForIndexing: Optional[bool] = None

    def _getGRPC(self):
        return schema.ReplicationNullableSettings(
            replica=NullableBool(self.replica)._getGRPC(),
            primaryDatabase=NullableString(self.primaryDatabase)._getGRPC(),
            primaryHost=NullableString(self.primaryHost)._getGRPC(),
            primaryPort=NullableUint32(self.primaryPort)._getGRPC(),
            primaryUsername=NullableString(self.primaryUsername)._getGRPC(),
            primaryPassword=NullableString(self.primaryUsername)._getGRPC(),
            syncReplication=NullableBool(self.syncReplication)._getGRPC(),
            syncAcks=NullableUint32(self.syncAcks)._getGRPC(),
            prefetchTxBufferSize=NullableUint32(
                self.prefetchTxBufferSize)._getGRPC(),
            replicationCommitConcurrency=NullableUint32(
                self.replicationCommitConcurrency)._getGRPC(),
            allowTxDiscarding=NullableBool(self.allowTxDiscarding)._getGRPC(),
            skipIntegrityCheck=NullableBool(
                self.skipIntegrityCheck)._getGRPC(),
            waitForIndexing=NullableBool(self.waitForIndexing)._getGRPC(),
        )


@dataclass
class TruncationSettings(GRPCTransformable):
    retentionPeriod: Optional[int]
    truncationFrequency: Optional[int]

    def _getGRPC(self):
        return schema.TruncationNullableSettings(
            retentionPeriod=NullableMilliseconds(
                self.retentionPeriod)._getGRPC(),
            truncationFrequency=NullableMilliseconds(
                self.truncationFrequency)._getGRPC(),
        )


@dataclass
class IndexSettings(GRPCTransformable):
    flushThreshold:  Optional[int] = None
    syncThreshold:  Optional[int] = None
    cacheSize:  Optional[int] = None
    maxNodeSize:  Optional[int] = None
    maxActiveSnapshots:  Optional[int] = None
    renewSnapRootAfter:  Optional[int] = None
    compactionThld:  Optional[int] = None
    delayDuringCompaction:  Optional[int] = None
    nodesLogMaxOpenedFiles:  Optional[int] = None
    historyLogMaxOpenedFiles:  Optional[int] = None
    commitLogMaxOpenedFiles:  Optional[int] = None
    flushBufferSize:  Optional[int] = None
    cleanupPercentage:  Optional[float] = None
    maxBulkSize: Optional[int] = None
    bulkPreparationTimeout: Optional[int] = None

    def _getGRPC(self):
        return schema.IndexNullableSettings(
            flushThreshold=NullableUint32(self.flushThreshold)._getGRPC(),
            syncThreshold=NullableUint32(self.syncThreshold)._getGRPC(),
            cacheSize=NullableUint32(self.cacheSize)._getGRPC(),
            maxNodeSize=NullableUint32(self.maxNodeSize)._getGRPC(),
            maxActiveSnapshots=NullableUint32(
                self.maxActiveSnapshots)._getGRPC(),
            renewSnapRootAfter=NullableUint64(
                self.renewSnapRootAfter)._getGRPC(),
            compactionThld=NullableUint32(self.compactionThld)._getGRPC(),
            delayDuringCompaction=NullableUint32(
                self.delayDuringCompaction)._getGRPC(),
            nodesLogMaxOpenedFiles=NullableUint32(
                self.nodesLogMaxOpenedFiles)._getGRPC(),
            historyLogMaxOpenedFiles=NullableUint32(
                self.historyLogMaxOpenedFiles)._getGRPC(),
            commitLogMaxOpenedFiles=NullableUint32(
                self.commitLogMaxOpenedFiles)._getGRPC(),
            flushBufferSize=NullableUint32(self.flushBufferSize)._getGRPC(),
            cleanupPercentage=NullableFloat(self.cleanupPercentage)._getGRPC(),
            maxBulkSize=NullableUint32(self.maxBulkSize)._getGRPC(),
            bulkPreparationTimeout=NullableMilliseconds(
                self.bulkPreparationTimeout)._getGRPC(),
        )


@dataclass
class AHTSettings(GRPCTransformable):
    syncThreshold: Optional[int] = None
    writeBufferSize: Optional[int] = None

    def _getGRPC(self):
        return schema.AHTNullableSettings(
            syncThreshold=NullableUint32(self.syncThreshold)._getGRPC(),
            writeBufferSize=NullableUint32(self.writeBufferSize)._getGRPC()
        )


@dataclass
class DatabaseSettingsV2(GRPCTransformable):
    replicationSettings:  ReplicationSettings = None
    fileSize: Optional[int] = None
    maxKeyLen: Optional[int] = None
    maxValueLen: Optional[int] = None
    maxTxEntries: Optional[int] = None
    excludeCommitTime: Optional[bool] = None
    maxConcurrency:  Optional[int] = None
    maxIOConcurrency:  Optional[int] = None
    txLogCacheSize:  Optional[int] = None
    vLogMaxOpenedFiles: Optional[int] = None
    txLogMaxOpenedFiles: Optional[int] = None
    commitLogMaxOpenedFiles: Optional[int] = None
    indexSettings: IndexSettings = None
    writeTxHeaderVersion:  Optional[int] = None
    autoload: Optional[bool] = None
    readTxPoolSize: Optional[int] = None
    syncFrequency:  NullableMilliseconds = None
    writeBufferSize:  Optional[int] = None
    ahtSettings:  AHTSettings = None
    maxActiveTransactions: Optional[int] = None
    mvccReadSetLimit: Optional[int] = None
    vLogCacheSize: Optional[int] = None
    truncationSettings: TruncationSettings = None
    embeddedValues: Optional[bool] = None
    preallocFiles: Optional[bool] = None

    def _getGRPC(self):
        indexSettings = None
        if self.indexSettings != None:
            indexSettings = self.indexSettings._getGRPC()

        replicationSettings = None
        if self.replicationSettings != None:
            replicationSettings = self.replicationSettings._getGRPC()

        ahtSettings = None
        if self.ahtSettings != None:
            ahtSettings = self.ahtSettings._getGRPC()

        truncSettings = None
        if self.truncationSettings != None:
            truncSettings = self.truncationSettings._getGRPC()

        return schema.DatabaseNullableSettings(
            replicationSettings=replicationSettings,
            fileSize=NullableUint32(self.fileSize)._getGRPC(),
            maxKeyLen=NullableUint32(self.maxKeyLen)._getGRPC(),
            maxValueLen=NullableUint32(self.maxValueLen)._getGRPC(),
            maxTxEntries=NullableUint32(self.maxTxEntries)._getGRPC(),
            excludeCommitTime=NullableBool(self.excludeCommitTime)._getGRPC(),
            maxConcurrency=NullableUint32(self.maxConcurrency)._getGRPC(),
            maxIOConcurrency=NullableUint32(self.maxIOConcurrency)._getGRPC(),
            txLogCacheSize=NullableUint32(self.txLogCacheSize)._getGRPC(),
            vLogMaxOpenedFiles=NullableUint32(
                self.vLogMaxOpenedFiles)._getGRPC(),
            txLogMaxOpenedFiles=NullableUint32(
                self.txLogMaxOpenedFiles)._getGRPC(),
            commitLogMaxOpenedFiles=NullableUint32(
                self.commitLogMaxOpenedFiles)._getGRPC(),
            indexSettings=indexSettings,
            writeTxHeaderVersion=NullableUint32(
                self.writeTxHeaderVersion)._getGRPC(),
            autoload=NullableBool(self.autoload)._getGRPC(),
            readTxPoolSize=NullableUint32(self.readTxPoolSize)._getGRPC(),
            syncFrequency=NullableMilliseconds(self.syncFrequency)._getGRPC(),
            writeBufferSize=NullableUint32(self.writeBufferSize)._getGRPC(),
            ahtSettings=ahtSettings,
            maxActiveTransactions=NullableUint32(
                self.maxActiveTransactions)._getGRPC(),
            mvccReadSetLimit=NullableUint32(self.mvccReadSetLimit)._getGRPC(),
            vLogCacheSize=NullableUint32(self.vLogCacheSize)._getGRPC(),
            truncationSettings=truncSettings,
            embeddedValues=NullableBool(self.embeddedValues)._getGRPC(),
            preallocFiles=NullableBool(self.preallocFiles)._getGRPC(),
        )


@dataclass
class ReplicationNullableSettings(GRPCTransformable):
    replica:  NullableBool = None
    primaryDatabase:  NullableString = None
    primaryHost:  NullableString = None
    primaryPort:  NullableUint32 = None
    primaryUsername: NullableString = None
    primaryPassword: NullableString = None
    syncReplication: NullableBool = None
    syncAcks: NullableUint32 = None
    prefetchTxBufferSize: NullableUint32 = None,
    replicationCommitConcurrency: NullableUint32 = None,
    allowTxDiscarding: NullableBool = None,
    skipIntegrityCheck: NullableBool = None,
    waitForIndexing: NullableBool = None,

    def _getHumanDataClass(self):
        return grpcHumanizator(self, ReplicationSettings)


@dataclass
class IndexNullableSettings(GRPCTransformable):
    flushThreshold:  NullableUint32 = None
    syncThreshold:  NullableUint32 = None
    cacheSize:  NullableUint32 = None
    maxNodeSize:  NullableUint32 = None
    maxActiveSnapshots:  NullableUint32 = None
    renewSnapRootAfter:  NullableUint64 = None
    compactionThld:  NullableUint32 = None
    delayDuringCompaction:  NullableUint32 = None
    nodesLogMaxOpenedFiles:  NullableUint32 = None
    historyLogMaxOpenedFiles:  NullableUint32 = None
    commitLogMaxOpenedFiles:  NullableUint32 = None
    flushBufferSize:  NullableUint32 = None
    cleanupPercentage:  NullableFloat = None
    maxBulkSize: NullableUint32 = None
    bulkPreparationTimeout: NullableMilliseconds = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, IndexSettings)


@dataclass
class AHTNullableSettings(GRPCTransformable):
    syncThreshold: NullableUint32 = None
    writeBufferSize: NullableUint32 = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, AHTSettings)


@dataclass
class LoadDatabaseRequest(GRPCTransformable):
    database: str = None


@dataclass
class LoadDatabaseResponse(GRPCTransformable):
    database: str = None


@dataclass
class UnloadDatabaseRequest(GRPCTransformable):
    database: str = None


@dataclass
class UnloadDatabaseResponse(GRPCTransformable):
    database: str = None


@dataclass
class DeleteDatabaseRequest(GRPCTransformable):
    database: str = None


@dataclass
class DeleteDatabaseResponse(GRPCTransformable):
    database: str = None


@dataclass
class FlushIndexRequest(GRPCTransformable):
    cleanupPercentage: float = None
    synced: bool = None


@dataclass
class FlushIndexResponse(GRPCTransformable):
    database: str = None


@dataclass
class Table(GRPCTransformable):
    tableName: str = None


@dataclass
class SQLGetRequest(GRPCTransformable):
    table: str = None
    pkValues: List[SQLValue] = None
    atTx: int = None
    sinceTx: int = None


@dataclass
class VerifiableSQLGetRequest(GRPCTransformable):
    sqlGetRequest: SQLGetRequest = None
    proveSinceTx: int = None


@dataclass
class SQLEntry(GRPCTransformable):
    tx: int = None
    key: bytes = None
    value: bytes = None
    metadata: KVMetadata = None


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
    verified: bool = False


@dataclass
class UseDatabaseReply(GRPCTransformable):
    token: str = None


class PermissionAction(Enum):
    GRANT = 0
    REVOK = 1


@dataclass
class ChangePermissionRequest(GRPCTransformable):
    action: PermissionAction = None
    username: str = None
    database: str = None
    permission: int = None


@dataclass
class SetActiveUserRequest(GRPCTransformable):
    active: bool = None
    username: str = None


@dataclass
class DatabaseListResponse(GRPCTransformable):
    databases: List[Database] = None


@dataclass
class DatabaseListRequestV2(GRPCTransformable):
    pass


@dataclass
class DatabaseListResponseV2(GRPCTransformable):
    databases: List[Union[DatabaseWithSettings, DatabaseInfo]] = None

    def _getHumanDataClass(self):
        return DatabaseListResponseV2(databases=[toConvert._getHumanDataClass() for toConvert in self.databases])


@dataclass
class DatabaseWithSettings(GRPCTransformable):
    name: str = None
    settings: DatabaseNullableSettings = None
    loaded: bool = None

    def _getHumanDataClass(self):
        return grpcHumanizator(self, DatabaseInfo)


@dataclass
class DatabaseInfo(GRPCTransformable):
    name: str = None
    settings: DatabaseSettingsV2 = None
    loaded: bool = None
    diskSize: int = None
    numTransactions: int = None


@dataclass
class Chunk(GRPCTransformable):
    content: bytes = None


@dataclass
class UseSnapshotRequest(GRPCTransformable):
    sinceTx: int = None
    asBeforeTx: int = None


@dataclass
class SQLExecRequest(GRPCTransformable):
    sql: str = None
    params: List[NamedParam] = None
    noWait: bool = None


@dataclass
class SQLQueryRequest(GRPCTransformable):
    sql: str = None
    params: List[NamedParam] = None
    reuseSnapshot: int = None


@dataclass
class NamedParam(GRPCTransformable):
    name: str = None
    value: SQLValue = None


@dataclass
class SQLExecResult(GRPCTransformable):
    txs: List[CommittedSQLTx] = None
    ongoingTx: bool = None


@dataclass
class CommittedSQLTx(GRPCTransformable):
    header: TxHeader = None
    updatedRows: int = None
    lastInsertedPKs: Dict[str, SQLValue] = None
    firstInsertedPKs: Dict[str, SQLValue] = None


@dataclass
class SQLQueryResult(GRPCTransformable):
    columns: List[Column] = None
    rows: List[Row] = None


@dataclass
class Column(GRPCTransformable):
    name: str = None
    type: str = None


@dataclass
class Row(GRPCTransformable):
    columns: List[str] = None
    values: List[SQLValue] = None

# ONE OF


@dataclass
class SQLValue(GRPCTransformable):
    null: Optional[NullValue] = None
    n: Optional[int] = None
    s: Optional[str] = None
    b: Optional[bool] = None
    bs: Optional[bytes] = None
    ts: Optional[int] = None


@dataclass
class PrimaryKey:
    pass


@dataclass
class PrimaryKeyNullValue(GRPCTransformable, PrimaryKey):
    def _getGRPC(self):
        return schema.SQLValue(null=None)


@dataclass
class PrimaryKeyIntValue(GRPCTransformable, PrimaryKey):
    value: int

    def _getGRPC(self):
        return schema.SQLValue(n=self.value)


@dataclass
class PrimaryKeyVarCharValue(GRPCTransformable, PrimaryKey):
    value: str

    def _getGRPC(self):
        return schema.SQLValue(s=self.value)


@dataclass
class PrimaryKeyBoolValue(GRPCTransformable, PrimaryKey):
    value: bool

    def _getGRPC(self):
        return schema.SQLValue(b=self.value)


@dataclass
class PrimaryKeyBlobValue(GRPCTransformable, PrimaryKey):
    value: bytes

    def _getGRPC(self):
        return schema.SQLValue(bs=self.value)


@dataclass
class PrimaryKeyTsValue(GRPCTransformable, PrimaryKey):
    value: int

    def _getGRPC(self):
        return schema.SQLValue(ts=self.value)


class TxMode(Enum):
    ReadOnly = 0
    WriteOnly = 1
    ReadWrite = 2


@dataclass
class NewTxRequest(GRPCTransformable):
    mode: TxMode = None


@dataclass
class NewTxResponse(GRPCTransformable):
    transactionID: str = None


@dataclass
class ErrorInfo(GRPCTransformable):
    code: str = None
    cause: str = None


@dataclass
class DebugInfo(GRPCTransformable):
    stack: str = None


@dataclass
class RetryInfo(GRPCTransformable):
    retry_delay: int = None
