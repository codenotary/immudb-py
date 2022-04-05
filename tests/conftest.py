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
            client = ImmudbClient(
                publicKeyFile="tests/certs/pub.key.pem")
        else:
            client = ImmudbClient(*args, **kwargs)
            client.login("immudb", "immudb")
    except grpc.RpcError as e:
        pytest.skip("Cannot reach immudb server")
        return
    return client


@pytest.fixture(scope="function", params=["localhost:3322", "localhost:3333"])
def client_rs(rootfile, request):
    return client_margs(rs=PersistentRootService(rootfile), immudUrl=request.param)


@pytest.fixture(scope="function", params=["localhost:3322", "localhost:3333"])
def client_pem(request):
    return client_margs(pem=True, immudUrl=request.param)


@pytest.fixture(scope="function", params=["localhost:3322", "localhost:3333"])
def client(request):
    return client_margs(immudUrl=request.param)
