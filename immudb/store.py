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

SHA256LEN=32
import immudb.htree
import copy
import struct
import hashlib
from immudb.constants import *

class printable:
    def __repr__(self)->str:
        ret="class {}\n".format(type(self).__name__)
        for k in self.__dict__:
            v=self.__dict__[k]
            if type(v)==bytes:
                ret+="\t{}: {}\n".format(k,list(v))
            elif hasattr(v,'__getitem__') and type(v[0])==bytes:
                ret+="\t{}:\n".format(k)
                for t in v:
                    ret+="\t- {}\n".format(list(t))
            else:
                ret+="\t{}: {}\n".format(k,v)
        return ret
    

class TXe(printable):
    def __init__(self):
        self.keyLen=None
        self.key=None
        self.ValueLen=None
        self.hValue=None
        self.vOff=None
    def SetKey(self, key: bytes):
        self.key=copy.copy(key)
        self.keyLen=len(key)
    def Digest(self):
        b=self.key+self.hValue
        return hashlib.sha256(b).digest()


        
class Tx(printable):
    def __init__(self):
        self.ID=None
        self.Ts=None
        self.BlTxID=None
        self.BlRoot=None
        self.PrevAlh=None
        self.nentries=None
        self.entries=None     
        self.htree=None
        self.Alh=None
        self.InnerHash=None
    def BuildHashTree(self):
        digests=[]
        for e in self.entries:
            digests.append(e.Digest())
        self.htree.BuildWith(digests)
    def CalcAlh(self):
        self.calcInnerhash()
        bi=struct.pack(">Q",self.ID)+self.PrevAlh+self.InnerHash
        self.Alh=hashlib.sha256(bi).digest()
    def calcInnerhash(self):
        bj=struct.pack(">QL",self.Ts,self.nentries)+self.eh()
        bj+=struct.pack(">Q",self.BlTxID)+self.BlRoot
        self.InnerHash=hashlib.sha256(bj).digest()
    def eh(self):
        return self.htree.root
    def Proof(self, key:bytes):
        kindex=None
        # find index of element holding given key 
        #kindex=next(k for k,v in enumerate(self.entries) if v.key==key)
        for k,v in enumerate(self.entries):
            if v.key==key:
                kindex=k
                break
        if kindex==None:
            raise LookupError
        return self.htree.InclusionProof(kindex)

def NewTxWithEntries(entries:list) -> Tx:
    tx=Tx()
    tx.ID=0
    tx.entries=entries
    tx.nentries=len(entries)
    tx.htree=immudb.htree.HTree(len(entries))
    return tx
    
def TxFrom(stx) -> Tx:
    entries=[]
    for e in stx.entries:
        i=TXe()
        i.hValue=DigestFrom(e.hValue)
        i.vOff=e.vOff
        i.ValueLen=int(e.vLen)
        i.SetKey(e.key)
        entries.append(i)
    tx=NewTxWithEntries(entries)
    tx.ID = stx.metadata.id
    tx.PrevAlh = DigestFrom(stx.metadata.prevAlh)
    tx.Ts = stx.metadata.ts
    tx.BlTxID = stx.metadata.blTxId
    tx.BlRoot = DigestFrom(stx.metadata.blRoot)
    tx.BuildHashTree()
    tx.CalcAlh()
    return tx



class TxMetadata(printable):
    def __init__(self):
        self.iD       = None
        self.prevAlh  = None
        self.ts       = None
        self.nEntries = None
        self.eh       = None
        self.blTxID   = None
        self.blRoot   = None
    def alh(self):
        bi=struct.pack(">Q",self.iD)+self.prevAlh
        bj=struct.pack(">QL",int(self.ts),int(self.nEntries))
        bj=bj+self.eh+struct.pack(">Q",self.blTxID)+self.blRoot
        innerHash=hashlib.sha256(bj).digest()
        bi=bi+innerHash
        return hashlib.sha256(bi).digest()
    
def TxMetadataFrom(txmFrom):
    txm=TxMetadata()
    txm.iD       = txmFrom.id
    txm.prevAlh  = DigestFrom(txmFrom.prevAlh)
    txm.ts       = txmFrom.ts
    txm.nEntries = int(txmFrom.nentries)
    txm.eh       = DigestFrom(txmFrom.eH)
    txm.blTxID   = txmFrom.blTxId
    txm.blRoot   = DigestFrom(txmFrom.blRoot)
    return txm


    
class KV(printable):
    def __init__(self,key: bytes,value:bytes):
        self.key=key
        self.value=value
    def Digest(self):
        if self.value==None:
            valdigest=hashlib.sha256(b'').digest()
        else:
            valdigest=hashlib.sha256(self.value).digest()
        return hashlib.sha256(self.key+valdigest).digest()
        
def EncodeKey(key: bytes):
    return SET_KEY_PREFIX+key

def EncodeKV(key: bytes, value: bytes):
    return KV(SET_KEY_PREFIX+key,PLAIN_VALUE_PREFIX+value)

def EncodeReference(key:bytes, referencedKey: bytes, atTx: int):
    refVal=REFERENCE_VALUE_PREFIX+struct.pack(">Q",atTx)+SET_KEY_PREFIX+referencedKey
    return KV(SET_KEY_PREFIX+key,refVal)

def EncodeZAdd(zset:bytes, score:float, key: bytes, attx:int):
    ekey=SET_KEY_PREFIX+key
    zkey=SORTED_KEY_PREFIX
    zkey+=struct.pack(">Q",len(zset))+zset
    zkey+=struct.pack(">d",score)
    zkey+=struct.pack(">Q",len(ekey))+ekey
    zkey+=struct.pack(">Q",attx)
    return KV(zkey,None)


class LinearProof(printable):
    def __init__(self, sourceTxID:int, targetTxID:int, terms:list):
        self.sourceTxID=sourceTxID
        self.targetTxID=targetTxID
        self.terms=terms

def LinearProofFrom(lp)->LinearProof:
    return LinearProof(lp.sourceTxId, lp.TargetTxId, lp.terms)

def DigestFrom(slicedDigest: bytes)->bytes:
    d=copy.copy(slicedDigest[:SHA256LEN])
    return d

def DigestsFrom(slicedTerms: list) -> list:
    d=[copy.copy(i) for i in slicedTerms]
    return d
    


def VerifyInclusion(proof, digest: bytes, root) -> bool:
    if proof==None:
        return False
    leaf=LEAF_PREFIX+digest
    calcRoot = hashlib.sha256(leaf).digest()
    i=proof.leaf
    r=proof.width-1
    for t in proof.terms:
        b=NODE_PREFIX
        if i%2==0 and i!=r:
            b=b+calcRoot+t
        else:
            b=b+t+calcRoot
        calcRoot=hashlib.sha256(b).digest()
        i=i//2
        r=r//2
    return i==r and root==calcRoot

def leafFor(d:bytes)->bytes:
    b=LEAF_PREFIX+d
    return hashlib.sha256(b).digest()

def VerifyDualProof(proof, sourceTxID, targetTxID , sourceAlh, targetAlh):
    if (proof==None or
        proof.sourceTxMetadata==None or
        proof.targetTxMetadata==None or
        proof.sourceTxMetadata.iD != sourceTxID or
        proof.targetTxMetadata.iD != targetTxID):
            return False
    if proof.sourceTxMetadata.iD==0 or proof.sourceTxMetadata.iD > proof.targetTxMetadata.iD:
        return False
    if sourceAlh != proof.sourceTxMetadata.alh():
        return False
    if targetAlh != proof.targetTxMetadata.alh():
        return False
    if sourceTxID < proof.targetTxMetadata.blTxID and VerifyInclusionAHT( 
            proof.inclusionProof,
            sourceTxID,
            proof.targetTxMetadata.blTxID,
            leafFor(sourceAlh),
            proof.targetTxMetadata.blRoot)==False:
                return False
    if proof.sourceTxMetadata.blTxID > 0 and VerifyConsistency( 
            proof.consistencyProof,
            proof.sourceTxMetadata.blTxID,
            proof.targetTxMetadata.blTxID,
            proof.sourceTxMetadata.blRoot,
            proof.targetTxMetadata.blRoot)==False:
                return False
    if proof.targetTxMetadata.blTxID > 0 and VerifyLastInclusion( 
            proof.lastInclusionProof,
            proof.targetTxMetadata.blTxID,
            leafFor(proof.targetBlTxAlh),
            proof.targetTxMetadata.blRoot)==False:
                return False
    if sourceTxID < proof.targetTxMetadata.blTxID:
        ret=VerifyLinearProof(proof.linearProof, proof.targetTxMetadata.blTxID, targetTxID, proof.targetBlTxAlh, targetAlh) 
    else:
        ret=VerifyLinearProof(proof.linearProof, sourceTxID, targetTxID, sourceAlh, targetAlh)
    return ret

def VerifyInclusionAHT(iproof:list, i:int, j:int, iLeaf:bytes, jRoot:bytes) -> bool:
    if i>j or i==0 or i<j and len(iproof)==0:
        return False
    i1 = i - 1
    j1 = j - 1
    ciRoot = iLeaf
    for h in iproof:
        if i1%2 == 0 and i1 != j1:
            b=NODE_PREFIX+ciRoot+h
        else:
            b=NODE_PREFIX+h+ciRoot
        ciRoot = hashlib.sha256(b).digest()
        i1=i1>>1
        j1=j1>>1
    return jRoot==ciRoot

def VerifyConsistency(cproof:list, i:int, j:int, iRoot:bytes, jRoot:bytes)-> bool:
    if i > j or i == 0 or (i < j and len(cproof) == 0):
            return False
    if i == j and len(cproof) == 0:
            return iRoot == jRoot

    fn = i - 1
    sn = j - 1
    while fn%2 == 1:
        fn=fn>>1
        sn=sn>>1
    ciRoot, cjRoot = cproof[0], cproof[0]
    for h in cproof[1:]:
            if fn%2 == 1 or fn == sn:
                b=NODE_PREFIX+h+ciRoot
                ciRoot = hashlib.sha256(b).digest()
                b=NODE_PREFIX+h+cjRoot
                cjRoot = hashlib.sha256(b).digest()
                while fn%2 == 0 and fn != 0:
                    fn=fn>>1
                    sn=sn>>1
            else:
                b=NODE_PREFIX+cjRoot+h
                cjRoot=hashlib.sha256(b).digest()
            fn=fn>>1
            sn=sn>>1
    return iRoot == ciRoot and jRoot == cjRoot
    

def VerifyLastInclusion(iproof:list, i:int, leaf:bytes, root:bytes)->bool:
    if i==0:
        return False
    i1 = i - 1
    iroot = leaf
    for h in iproof:
        b=NODE_PREFIX+h+iroot
        iroot=hashlib.sha256(b).digest()
        i1 >>= 1
    return root==iroot
    
def VerifyLinearProof(proof , sourceTxID:int, targetTxID:int, sourceAlh:bytes, targetAlh:bytes) -> bool:
    if proof == None or proof.sourceTxID != sourceTxID or proof.targetTxID != targetTxID:
            return False

    if (proof.sourceTxID == 0 or proof.sourceTxID > proof.targetTxID or
            len(proof.terms) == 0 or sourceAlh != proof.terms[0]):
            return False

    calculatedAlh = proof.terms[0]
    for i in range(1,len(proof.terms)):
        bs=struct.pack(">Q",proof.sourceTxID+i)+calculatedAlh+proof.terms[i]
        calculatedAlh=hashlib.sha256(bs).digest()

    return targetAlh == calculatedAlh

    
    
    
