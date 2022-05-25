

from immudb import constants
from immudb.client import ImmudbClient
import uuid
from typing import List


class ImmuTestClient:
    def __init__(self, client: ImmudbClient):
        self.client = client
        self.transactionStart = "BEGIN TRANSACTION;"
        self.transactionEnd = "COMMIT;"
        self.currentTx = None

    def openSession(self, login, password, db):
        self.client.openSession(login, password, db)

    def newTx(self):
        interface = self.client.newTx()
        self.currentTx = interface
        return interface

    def commit(self):
        self.currentTx.commit()

    def rollback(self):
        self.currentTx.rollback()


    def openManagedSession(self, login, password, db):
        return self.client.openManagedSession(login, password, db)

    def get(self, key: bytes):
        return self.client.get(key)
        
    def set(self, key: bytes, value: bytes):
        return self.client.set(key, value)

    def closeSession(self):
        self.client.closeSession()

    def serverHigherOrEqualsToVersion(self, version: str):
        health = self.client.health()
        return self.compare_version(health.version, version) > -1

    def executeWithTransaction(self, concatenatedParams: dict, queries: List[str], separator="\n"):
        toExecute = [self.transactionStart]
        toExecute.extend(queries)
        toExecute.append(self.transactionEnd)
        multiLineQuery = separator.join(toExecute)
        resp = self.client.sqlExec(multiLineQuery, concatenatedParams)
        assert((len(resp.txs) > 0 and not resp.ongoingTx and not resp.UnknownFields())
               or len(resp.UnknownFields()) > 0)
        return resp

    def _generateTableName(self):
        return ("T" + str(uuid.uuid4()).replace("-", "")).lower()

    def generateKeyName(self):
        return ("K" + str(uuid.uuid4()).replace("-", "")).lower()

    # @TODO instead of generating table names it should just start container for every test / clean up everything
    def createTestTable(self, *fields: List[str]):
        tabname = self._generateTableName()
        fieldsJoined = ",".join(fields)
        if(self.currentTx):
            resp = self.currentTx.sqlExec(
            "CREATE TABLE {table} ({fieldsJoined});".format(
                table=tabname, fieldsJoined=fieldsJoined)
            )
            return tabname
        resp = self.client.sqlExec(
            "CREATE TABLE {table} ({fieldsJoined});".format(
                table=tabname, fieldsJoined=fieldsJoined)
        )
        assert((len(resp.txs) > 0 and not resp.ongoingTx and not resp.UnknownFields())
               or len(resp.UnknownFields()) > 0)

        resp = self.client.listTables()
        assert tabname in resp
        return tabname

    def prepareInsertQuery(self, table: str, fields: List[str], values: List):
        fieldsJoined = ",".join(fields)
        paramsJoined = ",".join(values)
        return "INSERT INTO {table} ({fieldsJoined}) VALUES ({paramsJoined});".format(table=table, fieldsJoined=fieldsJoined, paramsJoined=paramsJoined)

    def prepareSelectQuery(self, fromWhat: str, whatToSelect: List[str], conditions: List[str]):
        fieldsJoined = ",".join(whatToSelect)
        conditionsJoined = ",".join(conditions)

        constructed = "SELECT {fieldsJoined} FROM {table}".format(
            table=fromWhat, fieldsJoined=fieldsJoined)
        if(len(conditions) > 0):
            constructed = constructed + " WHERE " + conditionsJoined
        constructed = constructed + ";"

        return constructed

    # @TODO define possible types of values
    def insertToTable(self, table: str, fields: List[str], values: List, params: dict):
        preparedQuery = self.prepareInsertQuery(table, fields, values)
        if(self.currentTx):
            print(preparedQuery)
            return self.currentTx.sqlExec(preparedQuery, params)

        resp = self.client.sqlExec(preparedQuery, params)
        assert((len(resp.txs) > 0 and not resp.ongoingTx and not resp.UnknownFields())
               or len(resp.UnknownFields()) > 0)
        return resp

    def simpleSelect(self, fromWhat: str, whatToSelect: List[str], params: dict, *conditions: List[str], columnNameMode = constants.COLUMN_NAME_MODE_NONE):
        preparedQuery = self.prepareSelectQuery(
            fromWhat, whatToSelect, conditions)
        print(self.currentTx)
        if(self.currentTx):
            return self.currentTx.sqlQuery(preparedQuery, params, columnNameMode=columnNameMode)
        result = self.client.sqlQuery(preparedQuery, params, columnNameMode=columnNameMode)
        return result

    def compare_version(self, a: str, b: str):
        aNumber = a.split("-")
        bNumber = b.split("-")
        (aMajor, aMinor, aRev) = aNumber[0].split(".")
        (bMajor, bMinor, bRev) = bNumber[0].split(".")
        if (aMajor, aMinor, aRev) < (bMajor, bMinor, bRev):
            return -1
        elif (aMajor, aMinor, aRev) > (bMajor, bMinor, bRev):
            return 1
        else:
            return 0
