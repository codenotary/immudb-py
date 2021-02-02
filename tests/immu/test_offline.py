import pytest
import base64
import immudb.store, immudb.htree
from immudb import constants
from immudb.grpc import schema_pb2

v0=b'CnIIGhIg0IswQi+55M5xLZSEZUNnpSqoU7JSjtSgNZBlyCMK/3IYzfrjgAYgASogsgXOdHznBIOL0fRjDit+QmDn+9M5FZms8jTI5fHfcpIwGTogmXu3vjcP/kHZTXvT0O158Tx9A3ywjmHG0LOPxS5Bk9kSOwoLAHNhbGFjYWR1bGESIMTfI1H+rKu77CCQQ/ktaUmx/krECfmjHSg+Gy3Zc2NvGMyAgICAgICAASAL'
s1=b'CglkZWZhdWx0ZGIQGhogjEG+djOLorSgQUZ4uxZ6pwEOrQJDtZJA7EvAoA2KGLY='
v1=b'CrABCnIIGxIgjEG+djOLorSgQUZ4uxZ6pwEOrQJDtZJA7EvAoA2KGLYY/oLkgAYgASogW42DIq7yEjr0Bd5vwhNgWawpXGugTjsyrCH5T/1aAgMwGjog+Ls6Bqhl3wY+Zm0bzdTSnefm2R5FmWbIBXVqI7+XK9USOgoKAGltbXV0YWJsZRIgNya+E7AWF0hes0nK+oPRxRM2QKuURi1RVts9Ls8n/p0Y14CAgICAgIABIAkS3AMKcggbEiCMQb52M4uitKBBRni7FnqnAQ6tAkO1kkDsS8CgDYoYthj+guSABiABKiBbjYMirvISOvQF3m/CE2BZrClca6BOOzKsIflP/VoCAzAaOiD4uzoGqGXfBj5mbRvN1NKd5+bZHkWZZsgFdWojv5cr1RJyCBsSIIxBvnYzi6K0oEFGeLsWeqcBDq0CQ7WSQOxLwKANihi2GP6C5IAGIAEqIFuNgyKu8hI69AXeb8ITYFmsKVxroE47Mqwh+U/9WgIDMBo6IPi7OgaoZd8GPmZtG83U0p3n5tkeRZlmyAV1aiO/lyvVIiBD2z2e5izoJX/f/wsxmRRgoqvRm9k1u07wnLvvKnnv7yIgvi9aTotJJoBOJNWBIamr92YFxY45RfWhIvDAjITo0qcqIIxBvnYzi6K0oEFGeLsWeqcBDq0CQ7WSQOxLwKANihi2MiDXuDDqI078k42ylczzX14lHfTDlrLjdJDHrqdhUD18YjIgcN967NBXNa3FCQYPp28QeNYB5oy92UyoZnJ9Z1Q5CDkyIL4vWk6LSSaATiTVgSGpq/dmBcWOOUX1oSLwwIyE6NKnOiYIGxAbGiAd/x/VhanAhIuG0lY4zYXyU650agKUGqi4trCM9MbPpg=='
s2=b'GBsiIB3/H9WFqcCEi4bSVjjNhfJTrnRqApQaqLi2sIz0xs+mKgA='
v2=b'CrABCnIIHBIgHf8f1YWpwISLhtJWOM2F8lOudGoClBqouLawjPTGz6YYvoTkgAYgASogW42DIq7yEjr0Bd5vwhNgWawpXGugTjsyrCH5T/1aAgMwGzogcdx7vXRTwJ2den3/Fj08IP1erEHl8a/8a+kd0I2a5/ASOgoKAGltbXV0YWJsZRIgNya+E7AWF0hes0nK+oPRxRM2QKuURi1RVts9Ls8n/p0Y4ICAgICAgIABIAkSwgQKcggbEiCMQb52M4uitKBBRni7FnqnAQ6tAkO1kkDsS8CgDYoYthj+guSABiABKiBbjYMirvISOvQF3m/CE2BZrClca6BOOzKsIflP/VoCAzAaOiD4uzoGqGXfBj5mbRvN1NKd5+bZHkWZZsgFdWojv5cr1RJyCBwSIB3/H9WFqcCEi4bSVjjNhfJTrnRqApQaqLi2sIz0xs+mGL6E5IAGIAEqIFuNgyKu8hI69AXeb8ITYFmsKVxroE47Mqwh+U/9WgIDMBs6IHHce710U8CdnXp9/xY9PCD9XqxB5fGv/GvpHdCNmufwIiB3UMOMPGOr9ivOCyKgp183KUPUsAazU5n+qTPGJbTGCyIgYUsXyKcZ+HVbCvm1CZ1SZMzIeq8bt47vbOrmzrpdp6AiIHDfeuzQVzWtxQkGD6dvEHjWAeaMvdlMqGZyfWdUOQg5IiC+L1pOi0kmgE4k1YEhqav3ZgXFjjlF9aEi8MCMhOjSpyogHf8f1YWpwISLhtJWOM2F8lOudGoClBqouLawjPTGz6YyIHdQw4w8Y6v2K84LIqCnXzcpQ9SwBrNTmf6pM8YltMYLMiBw33rs0Fc1rcUJBg+nbxB41gHmjL3ZTKhmcn1nVDkIOTIgvi9aTotJJoBOJNWBIamr92YFxY45RfWhIvDAjITo0qc6SAgbEBwaIB3/H9WFqcCEi4bSVjjNhfJTrnRqApQaqLi2sIz0xs+mGiAWMJsNvC67Pz/DKvwxULaaxae5nNHAH3A6EA3OwBC2zw=='

md0=b'CMsTEiCQ215KXNf4wwMRm3UNjdM9kCFOzoZ7OZwVhpQW5zYO7Bi6puWABiABKiAQXqDaVokfDIisvnvMOdPskYJ4L4NoeaU/fvQ8N0OsBjDKEzogZPn8RDVUgiakq0jkHrAJ/BpIMPJg6Hifz3U7pG+DE9g='

def test_verify_inclusion():
	key=b"salacadula"
	value=b"magicabula"

	vtx=schema_pb2.Tx()
	vtx.ParseFromString(base64.b64decode(v0))

	tx=immudb.store.TxFrom(vtx)
	inclusionProof=tx.Proof(constants.SET_KEY_PREFIX+key)
	ekv=immudb.store.EncodeKV(key, value)
	verifies=immudb.store.VerifyInclusion(inclusionProof, ekv.Digest(), tx.eh())
	assert verifies

def simulated_set(ss,vv,key,value):
	state=schema_pb2.ImmutableState()
	state.ParseFromString(base64.b64decode(ss))
	verifiableTx=schema_pb2.VerifiableTx()
	verifiableTx.ParseFromString(base64.b64decode(vv))
	tx=immudb.store.TxFrom(verifiableTx.tx)
	inclusionProof=tx.Proof(constants.SET_KEY_PREFIX+key)
	ekv=immudb.store.EncodeKV(key, value)
	verifies=immudb.store.VerifyInclusion(inclusionProof, ekv.Digest(), tx.eh())
	assert verifies
	assert tx.eh() == immudb.store.DigestFrom(verifiableTx.dualProof.targetTxMetadata.eH)
	if state.txId == 0:
		sourceID = tx.ID
		sourceAlh = tx.Alh
	else:
		sourceID = state.txId
		sourceAlh = immudb.store.DigestFrom(state.txHash)
	targetID = tx.ID
	targetAlh = tx.Alh

	assert immudb.store.VerifyDualProof(
		immudb.htree.DualProofFrom(verifiableTx.dualProof),
		sourceID,
		targetID,
		sourceAlh,
		targetAlh,
	)
	
def test_simulated_set1():
	simulated_set(s1,v1,b"immutable",b"database")
	
def test_simulated_set2():
	simulated_set(s2,v2,b"immutable",b"database")
	
def test_printable():
	verifiableTx=schema_pb2.VerifiableTx()
	verifiableTx.ParseFromString(base64.b64decode(v2))
	tx=immudb.store.TxFrom(verifiableTx.tx)
	s=repr(tx)
	assert type(s)==str
	txm=schema_pb2.TxMetadata()
	txm.ParseFromString(base64.b64decode(md0))
	s=repr(txm)
	assert type(s)==str

class FakeProof(object):
	pass
class FakeMetadata(object):
	def alh(self):
		return b'42'
	pass

def test_linearproof_fails():
	assert not immudb.store.VerifyLinearProof(None, 0,0, None, None)
	proof=FakeProof()
	proof.sourceTxID=0
	proof.targetTxID=0
	assert not immudb.store.VerifyLinearProof(proof, 0,0, None, None)

def test_lastinclusion_fails():
	assert not immudb.store.VerifyInclusion(None, [], 0)
	assert not immudb.store.VerifyLastInclusion([], 0, b'', b'')
	
def test_consistency_fails():
	assert not immudb.store.VerifyConsistency([], 0, 0, b'', b'')
	assert not immudb.store.VerifyConsistency([], 1, 1, b'1', b'')
	assert immudb.store.VerifyConsistency([], 1, 1, b'', b'')
	
def test_inclusionaht_fails():
	assert not immudb.store.VerifyInclusionAHT([], 0, 0, b'', b'')
	
def test_dualproof_fails():
	assert not immudb.store.VerifyDualProof(None, None, None, None, None)
	proof=FakeProof()
	proof.sourceTxMetadata=FakeMetadata()
	proof.targetTxMetadata=FakeMetadata()
	proof.sourceTxMetadata.iD=0
	proof.targetTxMetadata.iD=0
	assert not immudb.store.VerifyDualProof(proof, 0, 0, b'', b'')
	proof.sourceTxMetadata.iD=5
	proof.targetTxMetadata.iD=5
	assert not immudb.store.VerifyDualProof(proof, 5, 5, b'', b'')
	assert not immudb.store.VerifyDualProof(proof, 5, 5, b'42', b'1')
	proof.inclusionProof=b''
	proof.consistencyProof=b''
	proof.linearProof=b''
	proof.lastInclusionProof=b''
	proof.sourceTxMetadata.blTxID=6
	proof.targetTxMetadata.blTxID=6
	proof.sourceTxMetadata.blRoot=b''
	proof.targetTxMetadata.blRoot=b''
	assert not immudb.store.VerifyDualProof(proof, 5, 5, b'42', b'42')
	proof.targetTxMetadata.blTxID=0
	assert not immudb.store.VerifyDualProof(proof, 5, 5, b'42', b'42')
	proof.targetTxMetadata.blTxID=4
	proof.sourceTxMetadata.blTxID=4
	proof.targetBlTxAlh=b'42'
	assert not immudb.store.VerifyDualProof(proof, 5, 5, b'42', b'42')

def test_htree():
	h=immudb.htree.HTree(0)
	assert not hasattr(h,'levels')
	h=immudb.htree.HTree(8)
	assert hasattr(h,'levels')
	dig=[b'42',b'8853',b'ivoasiuyf',b'a0ds9zcv']
	h.BuildWith(dig)
	assert h.InclusionProof(1)
	dig=[b'42',b'8853',b'ivoasiuyf',b'a0ds9zcv',b'zotopac']
	h.BuildWith(dig)
	assert h.InclusionProof(1)
