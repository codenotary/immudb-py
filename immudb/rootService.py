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

from immudb import constants
from immudb.grpc import schema_pb2, schema_pb2_grpc
from google.protobuf import empty_pb2 as g_empty
import pickle, os.path, hashlib, ecdsa, ecdsa.util, struct
from dataclasses import dataclass

_statefile=constants.ROOT_CACHE_PATH


@dataclass
class State(object):
    db: str
    txId: int
    txHash: bytes
    publicKey: bytes
    signature: bytes
    @staticmethod
    def FromGrpc(grpcState):
        rs=State(
            db=grpcState.db,
            txId=grpcState.txId,
            txHash=grpcState.txHash,
            publicKey=grpcState.signature.publicKey,
            signature=grpcState.signature.signature,
            )
        return rs
    def Hash(self):
        b=struct.pack(">I",len(self.db))+self.db.encode('utf8')
        b+=struct.pack(">Q",self.txId)+self.txHash
        return b
    def Verify(self, verifying_key):
        verifying_key.verify(self.signature, self.Hash(), hashlib.sha256, sigdecode=ecdsa.util.sigdecode_der)


# Sample reference implementation, with no state persistance on disk.
# Can be used when instancing more that one client, in order to
# avoid concurrency issues
class RootService:
    def __init__(self):
        self.__dbname =None
        self.__cache = None
        self.__service = None
        
    def init(self, dbname:str, service: schema_pb2_grpc.ImmuServiceStub):
        self.__dbname = dbname
        self.__service = service
        state = self.__service.CurrentState(g_empty.Empty())
        self.__cache = state

    def get(self) -> State:
        if self.__cache==None:
            self.__cache=self.__service.CurrentState(g_empty.Empty())
        return self.__cache

    def set(self, root: State):
        self.__cache=root
        
# Sample implementation of persistent state. A state file is created, with 
# a dictionary of immudb states, one per database. State file name can be
# set at object creation, will use a default one if name is not set.
# Note: this implementation is not working correctly if more than one
# instance is using the same state file.
# Note: this implementation is not thread/process safe.
class PersistentRootService(RootService):
    def __init__(self, filename:str=None):
        self.__cache = None
        if filename!=None:
            self.__filename = filename
        else:
            self.__filename=os.path.join(os.path.expanduser("~"),_statefile)

    def init(self, dbname:str, service: schema_pb2_grpc.ImmuServiceStub):
        self.__dbname = dbname
        self.__service = service
        self.__cache=None
        try:
            with open(self.__filename, "rb") as f:
                states=pickle.load(f)
                if self.__dbname in states:
                    self.__cache=states[self.__dbname]
                    # IMPROVEMENT: check state validity (TODO)
        except FileNotFoundError:
            pass
        except Exception as e:
            print("Warning:",e)
        if self.__cache==None:
            self.__cache=self.__service.CurrentState(g_empty.Empty())

    def get(self) -> State:
        if self.__cache==None:
            # should never occour this
            self.__cache=self.__service.CurrentState(g_empty.Empty())
        return self.__cache

    def set(self, root: State):
        self.__cache=root
        states={}
        try:
            with open(self.__filename, "rb") as f:
                states=pickle.load(f)
        except Exception as e:
            print("Warning:",e)
        states[self.__dbname]=self.__cache
        with open(self.__filename, "wb") as f:
            pickle.dump(states,f)
