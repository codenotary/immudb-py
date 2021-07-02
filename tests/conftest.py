import os
import tempfile

import pytest

from immudb import ImmudbClient
import grpc._channel

from immudb.rootService import PersistentRootService


@pytest.fixture(scope="module")
def rootfile():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        yield f.name
    os.unlink(f.name)


def client_margs(*args, **kwargs):
    try:
        if 'pem' in kwargs and kwargs['pem']:
            client = ImmudbClient(publicKeyFile="tests/certs/pub.key.pem")
        else:
            client = ImmudbClient(*args, **kwargs)
            client.login("immudb", "immudb")
    except grpc._channel._InactiveRpcError as e:
        pytest.skip("Cannot reach immudb server")
        return
    return client


@pytest.fixture(scope="function")
def client_rs(rootfile):
    return client_margs(rs=PersistentRootService(rootfile))


@pytest.fixture(scope="function")
def client_pem():
    return client_margs(pem=True)


@pytest.fixture(scope="function")
def client():
    return client_margs()
