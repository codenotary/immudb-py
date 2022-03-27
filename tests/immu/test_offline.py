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

import pytest
import base64
from immudb.embedded import store, htree, ahtree
from immudb import constants
from immudb.grpc import schema_pb2
import immudb.database as database
import immudb.schema as schema
from immudb.exceptions import VerificationException

v0 = b'CnIIGhIg0IswQi+55M5xLZSEZUNnpSqoU7JSjtSgNZBlyCMK/3IYzfrjgAYgASogsgXOdHznBIOL0fRjDit+QmDn+9M5FZms8jTI5fHfcpIwGTogmXu3vjcP/kHZTXvT0O158Tx9A3ywjmHG0LOPxS5Bk9kSOwoLAHNhbGFjYWR1bGESIMTfI1H+rKu77CCQQ/ktaUmx/krECfmjHSg+Gy3Zc2NvGMyAgICAgICAASAL'
s1 = b'CglkZWZhdWx0ZGIaIOOwxEKY/BwUmvv0yJlvuSQnrkHkZJuTTKSVmRt4UrhV'
v1 = b'Cq4BCnAIARIg47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFUY4PiOgQYgASogW42DIq7yEjr0Bd5vwhNgWawpXGugTjsyrCH5T/1aAgM6IAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEjoKCgBpbW11dGFibGUSIDcmvhOwFhdIXrNJyvqD0cUTNkCrlEYtUVbbPS7PJ/6dGICAgICAgICAASAJEq4CCnAIARIg47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFUY4PiOgQYgASogW42DIq7yEjr0Bd5vwhNgWawpXGugTjsyrCH5T/1aAgM6IAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEnAIARIg47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFUY4PiOgQYgASogW42DIq7yEjr0Bd5vwhNgWawpXGugTjsyrCH5T/1aAgM6IAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADomCAEQARogQ09RbONv7xbgsma1KT1yMfx+yddpm40wcwoYv6hOkrI='
s2 = b'EAMaIGuuFuokAuSTnlONp9DUeKbqs9PsC6OPfPRHCFj3m7lkIgA='
v2 = b'CrABCnIIBBIga64W6iQC5JOeU42n0NR4puqz0+wLo4989EcIWPebuWQYuPmOgQYgASogW42DIq7yEjr0Bd5vwhNgWawpXGugTjsyrCH5T/1aAgMwAzogYbKA/+v2lwwwuJS2PGjSlFrtNrROcdOyTJMdcZejLYoSOgoKAGltbXV0YWJsZRIgNya+E7AWF0hes0nK+oPRxRM2QKuURi1RVts9Ls8n/p0Ym4CAgICAgIABIAkSugMKcggDEiDOLVre5lQhN8/n7u7GuJB4pawvDquH2hg4zB1yP2Mtlxi2+Y6BBiABKiBbjYMirvISOvQF3m/CE2BZrClca6BOOzKsIflP/VoCAzACOiBYiEJGiIuEIac70qeXsp6hTKp6y4WJzKOhRqaBXxyWIhJyCAQSIGuuFuokAuSTnlONp9DUeKbqs9PsC6OPfPRHCFj3m7lkGLj5joEGIAEqIFuNgyKu8hI69AXeb8ITYFmsKVxroE47Mqwh+U/9WgIDMAM6IGGygP/r9pcMMLiUtjxo0pRa7Ta0TnHTskyTHXGXoy2KIiBYiEJGiIuEIac70qeXsp6hTKp6y4WJzKOhRqaBXxyWIiIgE4/d3L0IM3aglxkuOYTcd3EZfeSwLdLPBI5szzkbcQ8qIGuuFuokAuSTnlONp9DUeKbqs9PsC6OPfPRHCFj3m7lkMiBYiEJGiIuEIac70qeXsp6hTKp6y4WJzKOhRqaBXxyWIjpICAMQBBoga64W6iQC5JOeU42n0NR4puqz0+wLo4989EcIWPebuWQaINYf7O7HFJ929+VRcR191s0ChS++OApbhJQARYuUMCfg'

md0 = b'CMsTEiCQ215KXNf4wwMRm3UNjdM9kCFOzoZ7OZwVhpQW5zYO7Bi6puWABiABKiAQXqDaVokfDIisvnvMOdPskYJ4L4NoeaU/fvQ8N0OsBjDKEzogZPn8RDVUgiakq0jkHrAJ/BpIMPJg6Hifz3U7pG+DE9g='


def test_verify_inclusion():
    key = b"salacadula"
    value = b"magicabula"

    vtx = schema_pb2.Tx()
    vtx.ParseFromString(base64.b64decode(v0))

    tx = schema.TxFromProto(vtx)
    entrySpecDigest = store.EntrySpecDigestFor(tx.header.version)
    inclusionProof = tx.Proof(database.EncodeKey(key))
    md = tx.entries[0].metadata()

    if md != None and md.Deleted():
        raise VerificationException

    e = database.EncodeEntrySpec(key, md, value)
    verifies = store.VerifyInclusion(
        inclusionProof, entrySpecDigest(e), tx.header.eh)
    assert verifies


def simulated_set(ss, vv, key, value):
    state = schema_pb2.ImmutableState()
    state.ParseFromString(base64.b64decode(ss))
    verifiableTx = schema_pb2.VerifiableTx()
    verifiableTx.ParseFromString(base64.b64decode(vv))
    tx = schema.TxFromProto(verifiableTx.tx)
    entrySpecDigest = store.EntrySpecDigestFor(tx.header.version)
    inclusionProof = tx.Proof(database.EncodeKey(key))
    md = tx.entries[0].metadata()

    if md != None and md.Deleted():
        raise VerificationException
    e = database.EncodeEntrySpec(key, md, value)
    verifies = store.VerifyInclusion(
        inclusionProof, entrySpecDigest(e), tx.header.eh)
    assert verifies
    assert tx.header.eh == schema.DigestFromProto(
        verifiableTx.dualProof.targetTxHeader.eH)
    # if state.txId == 0:
    #    sourceID = tx.ID
    #    sourceAlh = tx.Alh
    # else:
    #    sourceID = state.txId
    #    sourceAlh = immudb.embedded.store.DigestFrom(state.txHash)
    #targetID = tx.ID
    #targetAlh = tx.Alh
    # TODO: Check this
    if state.txId == 0:
        sourceID = tx.header.iD
        sourceAlh = tx.header.Alh()
    else:
        sourceID = state.txId
        sourceAlh = schema.DigestFromProto(state.txHash)
    targetID = tx.header.iD
    targetAlh = tx.header.Alh()

    assert store.VerifyDualProof(
        schema.DualProofFromProto(verifiableTx.dualProof),
        sourceID,
        targetID,
        sourceAlh,
        targetAlh,
    )


def test_simulated_set1():
    simulated_set(s1, v1, b"immutable", b"database")


def test_simulated_set2():
    simulated_set(s2, v2, b"immutable", b"database")


def test_printable():
    verifiableTx = schema_pb2.VerifiableTx()
    verifiableTx.ParseFromString(base64.b64decode(v2))
    tx = schema.TxFromProto(verifiableTx.tx)
    s = repr(tx)
    assert type(s) == str
    txm = schema_pb2.TxMetadata()
    txm.ParseFromString(base64.b64decode(md0))
    s = repr(txm)
    assert type(s) == str


class FakeProof(object):
    pass


class FakeMetadata(object):
    def Alh(self):
        return b'42'
    pass


def test_linearproof_fails():
    assert not store.VerifyLinearProof(None, 0, 0, None, None)
    proof = FakeProof()
    proof.sourceTxID = 0
    proof.targetTxID = 0
    assert not store.VerifyLinearProof(proof, 0, 0, None, None)


def test_lastinclusion_fails():
    assert not store.VerifyInclusion(None, [], 0)
    assert not ahtree.VerifyLastInclusion([], 0, b'', b'')


def test_consistency_fails():
    assert not ahtree.VerifyConsistency([], 0, 0, b'', b'')
    assert not ahtree.VerifyConsistency([], 1, 1, b'1', b'')
    assert ahtree.VerifyConsistency([], 1, 1, b'', b'')


def test_inclusionaht_fails():
    assert not ahtree.VerifyInclusion([], 0, 0, b'', b'')


def test_dualproof_fails():
    assert not store.VerifyDualProof(
        None, None, None, None, None)
    proof = FakeProof()
    proof.sourceTxHeader = FakeMetadata()
    proof.targetTxHeader = FakeMetadata()
    proof.sourceTxHeader.iD = 0
    proof.targetTxHeader.iD = 0
    assert not store.VerifyDualProof(proof, 0, 0, b'', b'')
    proof.sourceTxHeader.iD = 5
    proof.targetTxHeader.iD = 5
    assert not store.VerifyDualProof(proof, 5, 5, b'', b'')
    assert not store.VerifyDualProof(proof, 5, 5, b'42', b'1')
    proof.inclusionProof = b''
    proof.consistencyProof = b''
    proof.linearProof = b''
    proof.lastInclusionProof = b''
    proof.sourceTxHeader.blTxID = 6
    proof.targetTxHeader.blTxID = 6
    proof.sourceTxHeader.blRoot = b''
    proof.targetTxHeader.blRoot = b''
    assert not store.VerifyDualProof(proof, 5, 5, b'42', b'42')
    proof.targetTxHeader.blTxID = 0
    assert not store.VerifyDualProof(proof, 5, 5, b'42', b'42')
    proof.targetTxHeader.blTxID = 4
    proof.sourceTxHeader.blTxID = 4
    proof.targetBlTxAlh = b'42'
    assert not store.VerifyDualProof(proof, 5, 5, b'42', b'42')


def test_htree():
    h = htree.HTree(0)
    assert not hasattr(h, 'levels')
    h = htree.HTree(8)
    assert hasattr(h, 'levels')
    dig = [b'42', b'8853', b'ivoasiuyf', b'a0ds9zcv']
    h.BuildWith(dig)
    assert h.InclusionProof(1)
    dig = [b'42', b'8853', b'ivoasiuyf', b'a0ds9zcv', b'zotopac']
    h.BuildWith(dig)
    assert h.InclusionProof(1)
