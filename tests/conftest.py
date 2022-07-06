import os
import tempfile

import pytest

from immudb import ImmudbClient
import grpc._channel

from immudb.rootService import PersistentRootService
from .immuTestClient import ImmuTestClient

# When testing locally, you can start your test servers like that:
# docker run --rm -d --health-cmd "immuadmin status" --health-interval 10s --health-timeout 5s --health-retries 5 -v $(pwd)/tests/certs/my.key.pem:/key.pem -p 3333:3322 codenotary/immudb:1.1.0 --signingKey=/key.pem
# docker run --rm -d --health-cmd "immuadmin status" --health-interval 10s --health-timeout 5s --health-retries 5 -v $(pwd)/tests/certs/my.key.pem:/key.pem -p 3322:3322 codenotary/immudb:1.2.2 --signingKey=/key.pem

# See .github/workflows/ci.yml for the automated tests

TESTURLS = ["localhost:3322", "localhost:3333", "localhost:3344"]


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


@pytest.fixture(scope="function", params=TESTURLS)
def client_rs(rootfile, request):
    return client_margs(rs=PersistentRootService(rootfile), immudUrl=request.param)


@pytest.fixture(scope="function", params=TESTURLS)
def client_pem(request):
    return client_margs(pem=True, immudUrl=request.param)


@pytest.fixture(scope="function", params=TESTURLS)
def client(request):
    return client_margs(immudUrl=request.param)

@pytest.fixture(scope="function", params=TESTURLS)
def wrappedClient(request):
    return ImmuTestClient(client_margs(immudUrl=request.param))

@pytest.fixture(scope="function", params=TESTURLS)
def argsToBuildClient(request):
    return (request.param, "immudb", "immudb")
