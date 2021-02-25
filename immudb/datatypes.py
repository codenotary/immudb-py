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

from dataclasses import dataclass

@dataclass
class SetResponse:
    id: int
    verified: bool

@dataclass
class SafeGetResponse:
    id: int
    key: bytes
    value: bytes
    timestamp: int
    verified: bool
    refkey: bytes

@dataclass
class historyResponseItem:
    key: bytes
    value: bytes
    tx: int

@dataclass
class GetResponse:
    tx: int
    key: bytes
    value: bytes

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

