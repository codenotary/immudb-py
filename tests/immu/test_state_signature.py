# Copyright 2026 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import dataclasses

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from immudb.client import ImmudbClient, _loadPublicKey
from immudb.rootService import State

PUBKEYFILE = "tests/certs/pub.key.pem"
PRIVKEYFILE = "tests/certs/my.key.pem"


def signedState(privkey):
    state = State(db="defaultdb", txId=42, txHash=b"\x01" * 32,
                  publicKey=b"", signature=b"")
    state.signature = privkey.sign(state.Hash(), ec.ECDSA(hashes.SHA256()))
    return state


def test_verify_valid_signature():
    privkey = ec.generate_private_key(ec.SECP256R1())
    signedState(privkey).Verify(privkey.public_key())


def test_verify_tampered_state():
    privkey = ec.generate_private_key(ec.SECP256R1())
    state = dataclasses.replace(signedState(privkey), txId=43)
    with pytest.raises(InvalidSignature):
        state.Verify(privkey.public_key())


def test_verify_wrong_key():
    state = signedState(ec.generate_private_key(ec.SECP256R1()))
    other = ec.generate_private_key(ec.SECP256R1()).public_key()
    with pytest.raises(InvalidSignature):
        state.Verify(other)


def test_verify_with_test_certs():
    with open(PRIVKEYFILE, "rb") as f:
        privkey = serialization.load_pem_private_key(f.read(), None)
    with open(PUBKEYFILE) as f:
        pubkey = _loadPublicKey(f.read())
    signedState(privkey).Verify(pubkey)


def test_load_key_str_and_bytes():
    with open(PUBKEYFILE, "rb") as f:
        pem = f.read()
    assert isinstance(_loadPublicKey(pem), ec.EllipticCurvePublicKey)
    assert isinstance(_loadPublicKey(pem.decode("ascii")),
                      ec.EllipticCurvePublicKey)


def test_client_load_key():
    client = ImmudbClient("localhost:1", publicKeyFile=PUBKEYFILE)
    assert isinstance(client._vk, ec.EllipticCurvePublicKey)
    with open(PUBKEYFILE) as f:
        client.loadKeyFromString(f.read())
    assert isinstance(client._vk, ec.EllipticCurvePublicKey)


def test_load_key_rejects_non_ec():
    pem = rsa.generate_private_key(65537, 2048).public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo)
    with pytest.raises(ValueError):
        _loadPublicKey(pem)
