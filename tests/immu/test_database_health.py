from immudb import ImmudbClient
import datetime
from tests.immuTestClient import ImmuTestClient
import pytest

def test_database_health(wrappedClient: ImmuTestClient):
    client = wrappedClient.client
    if(not wrappedClient.serverHigherOrEqualsToVersion("1.2.0")):
        pytest.skip("Immudb version too low")
    timeNow = datetime.datetime.now()
    client.login("immudb", "immudb", "defaultdb")
    client.set(b"x", b"y")
    response1 = client.databaseHealth()
    assert response1.lastRequestCompletedAt > 0
    timeparsed = datetime.datetime.fromtimestamp(response1.lastRequestCompletedAt/1000)
    assert "pendingRequests" in response1.__dict__
    assert timeparsed > timeNow