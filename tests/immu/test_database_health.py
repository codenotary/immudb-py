from immudb import ImmudbClient
import datetime
def test_database_health(client: ImmudbClient):
    timeNow = datetime.datetime.now()
    client.login("immudb", "immudb", "defaultdb") # login into database is counted as request completed
    response1 = client.databaseHealth()
    assert response1.lastRequestCompletedAt > 0
    timeparsed = datetime.datetime.fromtimestamp(response1.lastRequestCompletedAt/1000)
    assert "pendingRequests" in response1.__dict__
    assert timeparsed > timeNow