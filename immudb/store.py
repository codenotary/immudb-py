#!/usr/bin/env python3
SHA256LEN=32
import immudb.htree
import copy
import struct
import hashlib
from immudb.constants import *

class TXe:
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
        
class Tx:
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
        # find index of element holding given key 
        kindex=next(k for k,v in enumerate(self.entries) if v.key==key)    
        return self.htree.InclusionProof(kindex)
    
    
def NewTxWithEntries(entries:list[TXe]) -> Tx:
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
    
def DigestFrom(slicedDigest: bytes)->bytes:
    d=copy.copy(slicedDigest[:SHA256LEN])
    return d


class KV:
    def __init__(self,key: bytes,value:bytes):
        self.key=key
        self.value=value
    def Digest(self):
        valdigest=hashlib.sha256(self.value).digest()
        return hashlib.sha256(self.key+valdigest).digest()
        
def EncodeKV(key: bytes, value: bytes):
    return KV(SET_KEY_PREFIX+key,PLAIN_VALUE_PREFIX+value)

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
        i=i/2
        r=r/2
    return i==r and root==calcRoot
