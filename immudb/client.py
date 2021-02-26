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

from immudb import header_manipulator_client_interceptor
from immudb.handler import (batchGet, batchSet, changePassword, createUser,
                          currentRoot, databaseCreate, databaseList, databaseUse, 
                          get, listUsers, verifiedGet, verifiedSet, setValue, history, 
                          scan, reference, verifiedreference, zadd, verifiedzadd, 
                          zscan, healthcheck, txbyid, verifiedtxbyid)
from immudb.rootService import *
from immudb.grpc import schema_pb2_grpc
import warnings, ecdsa

class ImmudbClient:
    def __init__(self, immudUrl=None, rs:RootService=None, publicKeyFile:str=None):
        if immudUrl is None:
            immudUrl = "localhost:3322"
        self.channel = grpc.insecure_channel(immudUrl)
        self.__stub = schema_pb2_grpc.ImmuServiceStub(self.channel)
        if rs==None:
            self.__rs=RootService()
        else:
            self.__rs=rs
        self.__url=immudUrl
        self.loadKey(publicKeyFile)
        
    def loadKey(self, kfile: str):
        if kfile==None:
            self.__vk = None
        else:
            with open(kfile) as f:
                self.__vk = ecdsa.VerifyingKey.from_pem(f.read())

            
    def login(self, username, password, database=b"defaultdb"):
        req = schema_pb2_grpc.schema__pb2.LoginRequest(user=bytes(
            username, encoding='utf-8'), password=bytes(
                password, encoding='utf-8'
                ))
        try:
            self.__login_response = schema_pb2_grpc.schema__pb2.LoginResponse = \
                self.__stub.Login(
                    req
                )
        except ValueError as e:
            raise Exception("Attempted to login on termninated client, channel has been shutdown") from e
            
        self.__stub = self.set_token_header_interceptor(self.__login_response)
        # Select database, modifying stub function accordingly
        request = schema_pb2_grpc.schema__pb2.Database(databasename=database)
        resp = self.__stub.UseDatabase(request)
        self.__stub = self.set_token_header_interceptor(resp)

        self.__rs.init("%s/%s".format(self.__url,database), self.__stub)
        return self.__login_response

    def shutdown(self):
        self.channel.close()
        self.channel = None
        self.intercept_channel.close
        self.intercept_channel = None
        self.__rs = None

    def set_token_header_interceptor(self, response):
        try:
            token = response.token
        except AttributeError:
            token = response.reply.token
        self.header_interceptor = \
            header_manipulator_client_interceptor.header_adder_interceptor(
                'authorization', "Bearer "+token
            )
        try:
            self.intercept_channel = grpc.intercept_channel(
                self.channel, self.header_interceptor)
        except ValueError as e:
            raise Exception("Attempted to login on termninated client, channel has been shutdown") from e
        return schema_pb2_grpc.ImmuServiceStub(self.intercept_channel)

    @property
    def stub(self):
        return self.__stub
    
    def healthCheck(self):
        return healthcheck.call(self.__stub, self.__rs)
        

    def get(self, key: bytes):
        return get.call(self.__stub, self.__rs, key)
    
    def getValue(self, key: bytes):
        ret=get.call(self.__stub, self.__rs, key)
        if ret==None:
            return None
        return ret.value

    def set(self, key: bytes, value: bytes):
        return setValue.call(self.__stub, self.__rs, key, value)

    def safeGet(self, key: bytes):
        warnings.warn("Call to deprecated safeGet. Use verifiedGet instead",
            category=DeprecationWarning,
            stacklevel=2
            )
        return verifiedGet.call(self.__stub, self.__rs, key, verifying_key=self.__vk)
    
    def verifiedGet(self, key: bytes):
        return verifiedGet.call(self.__stub, self.__rs, key, verifying_key=self.__vk)
    
    def verifiedGetAt(self, key: bytes, atTx:int):
        return verifiedGet.call(self.__stub, self.__rs, key, atTx, self.__vk)

    def safeSet(self, key: bytes, value: bytes):
        warnings.warn("Call to deprecated safeSet. Use verifiedSet instead",
            category=DeprecationWarning,
            stacklevel=2
            )
        return verifiedSet.call(self.__stub, self.__rs, key, value)
    
    def verifiedSet(self, key: bytes, value: bytes):
        return verifiedSet.call(self.__stub, self.__rs, key, value, self.__vk)

    def getAllValues(self, keys: list):
        resp = batchGet.call(self.__stub, self.__rs, keys)
        return resp

    def getAll(self, keys: list):
        resp = batchGet.call(self.__stub, self.__rs, keys)
        return {key:value.value for key, value in resp.items()}

    def setAll(self, kv: dict):
        return batchSet.call(self.__stub, self.__rs, kv)

    def changePassword(self, user, newPassword, oldPassword):
        request = schema_pb2_grpc.schema__pb2.ChangePasswordRequest(
                    user=bytes(user, encoding='utf-8'),                     
                    newPassword=bytes(newPassword, encoding='utf-8'),
                    oldPassword=bytes(oldPassword, encoding='utf-8')
                    )
        return changePassword.call(self.__stub, self.__rs, request)
    
    def createUser(self, user, password, permission, database):
        request = schema_pb2_grpc.schema__pb2.CreateUserRequest(
                    user=bytes(user, encoding='utf-8'),
                    password=bytes(password, encoding='utf-8'),
                    permission=permission,
                    database=database
                    )
        return createUser.call(self.__stub, self.__rs, request)
    
    def listUsers(self):
        return listUsers.call(self.__stub, None)
    
    def databaseList(self):
        dbs=databaseList.call(self.__stub, self.__rs, None)
        return [x.databasename for x in dbs.dblist.databases]

    def databaseUse(self, dbName: bytes):
        request = schema_pb2_grpc.schema__pb2.Database(databasename=dbName)
        resp = databaseUse.call(self.__stub, self.__rs, request)
        # modify header token accordingly
        self.__stub = self.set_token_header_interceptor(resp)
        self.__rs.init(dbName, self.__stub)
        return resp

    def databaseCreate(self, dbName: bytes):
        request = schema_pb2_grpc.schema__pb2.Database(databasename=dbName)
        return databaseCreate.call(self.__stub, self.__rs, request)

    def currentState(self):
        return currentRoot.call(self.__stub, self.__rs, None)

    def history(self, key: bytes, offset: int, limit: int, sortorder: bool):
        return history.call(self.__stub, self.__rs, key, offset, limit, sortorder)

    def logout(self):
        self.__stub.Logout(google_dot_protobuf_dot_empty__pb2.Empty())
        self.__login_response = None
        
    def scan(self, key:bytes, prefix:bytes, desc:bool, limit:int,sinceTx:int=None):
        return scan.call(self.__stub, self.__rs, key, prefix, desc, limit, sinceTx)
    
    def setReference(self, referredkey: bytes, newkey:  bytes):
        return reference.call(self.__stub, self.__rs, referredkey, newkey)
    
    def verifiedSetReference(self, referredkey: bytes, newkey:  bytes):
        return verifiedreference.call(self.__stub, self.__rs, referredkey, newkey, verifying_key=self.__vk)
    
    def zAdd(self, zset:bytes, score:float, key:bytes, atTx:int=0):
        return zadd.call(self.__stub, self.__rs, zset, score, key, atTx)
    
    def verifiedZAdd(self, zset:bytes, score:float, key:bytes, atTx:int=0):
        return verifiedzadd.call(self.__stub, self.__rs, zset, score, key, atTx, self.__vk)
    
    def zScan(self, zset:bytes, seekKey:bytes, seekScore:float,
                          seekAtTx:int, inclusive: bool, limit:int, desc:bool, minscore:float,
                          maxscore:float, sinceTx=None, nowait=False):
        return zscan.call(self.__stub, self.__rs, zset, seekKey, seekScore,
                          seekAtTx, inclusive, limit, desc, minscore,
                          maxscore, sinceTx, nowait)
                          
    def txById(self, tx:int):
        return txbyid.call(self.__stub, self.__rs, tx)
    
    def verifiedTxById(self, tx:int):
        return verifiedtxbyid.call(self.__stub, self.__rs, tx, self.__vk)
    
        
