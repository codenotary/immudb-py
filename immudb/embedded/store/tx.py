# Copyright 2022 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from immudb.embedded import store, htree
from immudb.printable import printable
import hashlib
from immudb.exceptions import ErrCorruptedData, ErrKeyNotFound, ErrMetadataUnsupported
import sys


def NewTxWithEntries(header: "TxHeader", entries: list) -> "Tx":

    _htree = htree.HTree(len(entries))

    tx = Tx()
    tx.header = header
    tx.entries = entries
    tx.htree = _htree
    return tx


class TxHeader(printable):
    def __init__(self):
        self.iD = None
        self.ts = None
        self.blTxID = None
        self.blRoot = None
        self.prevAlh = None

        self.version = None
        self.metadata = store.TxMetadata()

        self.nentries = None
        self.eh = None

    def innerHash(self) -> bytes:
        md = hashlib.sha256()
        md.update(self.ts.to_bytes(8, 'big'))
        md.update(self.version.to_bytes(2, 'big'))
        if self.version == 0:
            md.update(self.nentries.to_bytes(2, 'big'))
        elif self.version == 1:
            mdbs = b''
            if self.metadata != None:
                mdbs = self.metadata.Bytes()
                if mdbs == None:
                    mdbs = b''
            md.update(len(mdbs).to_bytes(2, 'big'))
            md.update(mdbs)
            md.update(self.nentries.to_bytes(4, 'big'))
        else:
            sys.exit("missing tx hash calculation method for version %d" %
                     self.version)
        md.update(self.eh)
        md.update(self.blTxID.to_bytes(8, 'big'))
        md.update(self.blRoot)
        return md.digest()

    def Alh(self) -> bytes:
        md = hashlib.sha256()
        md.update(self.iD.to_bytes(8, 'big'))
        md.update(self.prevAlh)
        md.update(self.innerHash())
        return md.digest()


class Tx(printable):
    def __init__(self):
        self.header = None
        self.entries = None
        self.htree = None

    def TxEntryDigest(self):
        if self.header.version == 0:
            return TxEntryDigest_v1_1
        elif self.header.version == 1:
            return TxEntryDigest_v1_2
        else:
            raise ErrCorruptedData

    def BuildHashTree(self):
        digests = []
        txEntryDigest = self.TxEntryDigest()
        for e in self.entries:
            digests.append(txEntryDigest(e))
        self.htree.BuildWith(digests)
        root = self.htree.root
        self.header.eh = root

    def IndexOf(self, key: bytes) -> int:
        kindex = None
        for k, v in enumerate(self.entries):
            if v.key() == key:
                kindex = k
                break
        if kindex == None:
            raise ErrKeyNotFound
        return k

    def Proof(self, key: bytes):
        kindex = self.IndexOf(key)
        return self.htree.InclusionProof(kindex)


class TxEntry(printable):
    def __init__(self, key: bytes, md: store.KVMetadata, vLen: int, hVal: bytes, vOff: int):
        self.k = key
        self.kLen = len(key)
        self.md = md
        self.vLen = vLen
        self.hVal = hVal
        self.vOff = vOff

    def key(self) -> bytes:
        return self.k

    def metadata(self) -> store.KVMetadata:
        return self.md


def TxEntryDigest_v1_1(e: TxEntry) -> bytes:
    if len(e.md.Bytes()) > 0:  # n.b. contrary to Golang, in Pyhton e.md will not be None
        raise ErrMetadataUnsupported
    md = hashlib.sha256()
    md.update(e.k)
    md.update(e.hVal)
    return md.digest()


def TxEntryDigest_v1_2(e: TxEntry) -> bytes:
    mdbs = b''
    if e.md != None:
        mdbs = e.md.Bytes()
    mdLen = len(mdbs)
    b = b''
    b = b+mdLen.to_bytes(2, 'big')
    b = b+mdbs
    b = b+e.kLen.to_bytes(2, 'big')
    b = b+e.k
    md = hashlib.sha256()
    md.update(b)
    md.update(e.hVal)
    return md.digest()
