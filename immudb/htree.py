#!/usr/bin/env python3
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
import hashlib
from immudb.constants import *
from immudb.exceptions import *
import immudb.store

class InclusionProof:
    def __init__(self):
        self.leaf=None
        self.width=None
        self.terms=b''

class DualProof:
    def __init__(self):
        self.sourceTxMetadata=None
        self.targetTxMetadata=None
        self.inclusionProof=None
        self.consistencyProof=None
        self.targetBlTxAlh=None
        self.lastInclusionProof=None
        self.linearProof=None
        
        
class HTree:
    def __init__(self, maxWidth:int):
        if maxWidth<1:
            return
       
        self.maxWidth=maxWidth
        lw=1
        while lw<maxWidth:
            lw=lw<<1
        height=(maxWidth-1).bit_length()+1
        self.levels=[None]*height
        for l in range(0,height):
            self.levels[l]=[None]*(lw>>l)

    def BuildWith(self, digests:list):
        if len(digests)>self.maxWidth:
            raise ErrMaxWidthExceeded
        if len(digests)==0:
            raise ErrIllegalArguments
        for i in range(0,len(digests)):
            leaf=LEAF_PREFIX+digests[i]
            self.levels[0][i]=hashlib.sha256(leaf).digest()
        l=0
        w=len(digests)
        while w>1:
            
            wn=0
            i=0
            while i+1<w:
                b=NODE_PREFIX+self.levels[l][i]+self.levels[l][i+1]
                self.levels[l+1][wn]=hashlib.sha256(b).digest()
                wn=wn+1
                i=i+2
            if w%2==1:
                self.levels[l+1][wn]=self.levels[l][w-1]
                wn=wn+1
            l+=1
            w=wn
        self.width=len(digests)
        self.root=self.levels[l][0]
    def InclusionProof(self, i):
        if i>=self.width:
            raise ErrIllegalArguments
        m=i
        n=self.width
        offset=0
        proof=InclusionProof()
        proof.leaf=i
        proof.width=self.width
        if self.width==1:
            return proof
        while True:
            d=(n-1).bit_length()
            k=1<<(d-1)
            if m<k:
                l,r=offset+k,offset+n-1
                n=k
            else:
                l,r=offset,offset+k-1
                m=m-k
                n=n-k
                offset=offset+k
            layer=(r-l).bit_length()
            index=int(l/(1<<layer))
            proof.terms=self.levels[layer][index]+proof.terms
            if n<1 or (n==1 and m==0):
                return proof
            
def InclusionProofFrom(iproof):
    h=InclusionProof()
    h.leaf=int(iproof.leaf)
    h.width=int(iproof.width)
    h.terms=immudb.store.DigestFrom(iproof.terms)
    return h

def DualProofFrom(dproof):
    dp=DualProof()
    dp.sourceTxMetadata=immudb.store.TxMetadataFrom(dproof.sourceTxMetadata)
    dp.targetTxMetadata=immudb.store.TxMetadataFrom(dproof.targetTxMetadata)
    dp.inclusionProof=immudb.store.DigestFrom(dproof.inclusionProof)
    dp.consistencyProof=immudb.store.DigestFrom(dproof.consistencyProof)
    dp.targetBlTxAlh=immudb.store.DigestFrom(dproof.targetBlTxAlh)
    dp.lastInclusionProof=immudb.store.DigestsFrom(dproof.lastInclusionProof)
    dp.linearProof=immudb.store.LinearProofFrom(dproof.linearProof) 
    return dp
