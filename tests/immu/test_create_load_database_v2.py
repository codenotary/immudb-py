from grpc import RpcError
from immudb import ImmudbClient
from immudb.datatypesv2 import CreateDatabaseRequest, DatabaseNullableSettings, NullableUint32, NullableBool
import uuid
import pytest


def test_create_database_v2(client: ImmudbClient):
    name = str(uuid.uuid4()).replace('-', '')
    settings = DatabaseNullableSettings(
        maxKeyLen=NullableUint32(32)
    )
    client.createDatabaseV2(name, settings, False)

    client.useDatabase(name.encode("utf-8"))
    client.set(('x' * 31).encode("utf-8"), b'x')

    with pytest.raises(RpcError):
        client.set(('x' * 32).encode("utf-8"), b'x')

def test_update_database_v2(client: ImmudbClient):
    name = str(uuid.uuid4()).replace('-', '')
    settings = DatabaseNullableSettings(
        maxKeyLen=NullableUint32(32)
    )
    client.createDatabaseV2(name, settings, False)

    client.useDatabase(name.encode("utf-8"))
    client.set(('x' * 31).encode("utf-8"), b'x')

    with pytest.raises(RpcError):
        client.set(('x' * 32).encode("utf-8"), b'x')
    
    settings = DatabaseNullableSettings(
        autoload=NullableBool(value = False)
    )
    resp = client.updateDatabaseV2(name, settings)

    resp = client.databaseListV2()
    foundDb = False
    # Important - GRPC is not sending False, it would send None
    for database in resp.databases:
        if database.settings.maxKeyLen.value == 32 and database.name == name and not database.settings.autoload.value:
            foundDb = True

    assert foundDb == True

    settings = DatabaseNullableSettings(
        autoload=NullableBool(value = True)
    )
    resp = client.updateDatabaseV2(name, settings)

    resp = client.databaseListV2()
    foundDb = False
    for database in resp.databases:
        if database.settings.maxKeyLen.value == 32 and database.name == name and database.settings.autoload.value:
            foundDb = True

    assert foundDb == True

def test_list_databases_v2(client: ImmudbClient):
    name = str(uuid.uuid4()).replace('-', '')
    settings = DatabaseNullableSettings(
        maxKeyLen=NullableUint32(32)
    )
    client.createDatabaseV2(name, settings, True)
    resp = client.databaseListV2()
    foundDb = False
    for database in resp.databases:
        if database.settings.maxKeyLen.value == 32 and database.name == name:
            foundDb = True

    assert foundDb == True

def test_load_unload_database(client: ImmudbClient):
    name = str(uuid.uuid4()).replace('-', '')
    settings = DatabaseNullableSettings(
        maxKeyLen=NullableUint32(32)
    )
    client.createDatabaseV2(name, settings, True)
    resp = client.unloadDatabase(name)
    assert resp.database == name
    with pytest.raises(RpcError):
        client.useDatabase(resp.database)

    resp = client.loadDatabase(name)
    assert resp.database == name
    client.useDatabase(resp.database)

def test_delete_database(client: ImmudbClient):
    name = str(uuid.uuid4()).replace('-', '')
    settings = DatabaseNullableSettings(
        maxKeyLen=NullableUint32(32)
    )
    client.createDatabaseV2(name, settings, True)
    client.useDatabase(name)
    resp = client.unloadDatabase(name)
    assert resp.database == name
    resp = client.deleteDatabase(name)
    assert resp.database == name
    with pytest.raises(RpcError):
        client.useDatabase(name)
    with pytest.raises(RpcError):
        resp = client.set(b"x", b"y")

def test_get_database_settings_v2(client: ImmudbClient):
    name = str(uuid.uuid4()).replace('-', '')
    settings = DatabaseNullableSettings(
        maxKeyLen=NullableUint32(32)
    )
    client.createDatabaseV2(name, settings, True)
    client.useDatabase(name)
    settings = client.getDatabaseSettingsV2()
    assert settings.database == name
    assert settings.settings.maxKeyLen.value == 32
        
