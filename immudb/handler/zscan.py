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

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb.exceptions import VerificationException

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, 
            zset:bytes, seekKey:bytes, seekScore:float,
            seekAtTx:int, inclusive: bool, limit:int, desc:bool, minscore:float,
            maxscore:float, sinceTx, nowait):
    request=schema_pb2.ZScanRequest(
        set=zset,
        seekKey = seekKey,
        seekScore = seekScore,
        seekAtTx = seekAtTx,
        inclusiveSeek = inclusive,
        limit = limit,
        desc = desc,
        minScore = schema_pb2.Score(score=minscore),
        maxScore = schema_pb2.Score(score=maxscore),
        sinceTx = sinceTx,
        noWait = nowait,
        )
    msg = service.ZScan(request)
    return msg
