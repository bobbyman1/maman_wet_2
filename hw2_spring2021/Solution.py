from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def errorHandler(e):
    ret_val = ReturnValue.ERROR
    if isinstance(e,DatabaseException.NOT_NULL_VIOLATION):
        ret_val = ReturnValue.BAD_PARAMS
    elif isinstance(e,DatabaseException.CHECK_VIOLATION):
        ret_val = ReturnValue.BAD_PARAMS
    elif isinstance(e,DatabaseException.UNIQUE_VIOLATION):
        ret_val = ReturnValue.ALREADY_EXISTS
    return ret_val




def createTable() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;" +
                     "CREATE TABLE Query\n" +
                     "(\n" +
                     "    id integer,\n" +
                     "    purpose text NOT NULL ,\n" +
                     "    disk_size_needed integer NOT NULL ,\n" +
                     "    PRIMARY KEY (id),\n" +
                     "    CHECK (id > 0),\n" +
                     "    CHECK (disk_size_needed >= 0)\n" +
                     ");" +

                     "CREATE TABLE Disk\n" +
                     "(\n" +
                     "    id integer,\n" +
                     "    manufacturing_company text NOT NULL ,\n" +
                     "    speed integer NOT NULL ,\n" +
                     "    free_space integer NOT NULL ,\n" +
                     "    cost_per_byte integer NOT NULL ,\n" +
                     "    PRIMARY KEY (id),\n" +
                     "    CHECK (id > 0),\n" +
                     "    CHECK (speed > 0),\n" +
                     "    CHECK (cost_per_byte > 0),\n" +
                     "    CHECK (free_space >= 0)\n" +
                     ");" +

                     "CREATE TABLE Ram\n" +
                     "(\n" +
                     "    id integer,\n" +
                     "    size integer NOT NULL ,\n" +
                     "    company text NOT NULL ,\n" +
                     "    PRIMARY KEY (id),\n" +
                     "    CHECK (id > 0),\n" +
                     "    CHECK (size > 0)\n" +
                     ");" +

                     "CREATE TABLE QueryOnDisk\n" +
                     "(\n" +
                     "    query_id integer,\n" +
                     "    disk_id integer,\n"
                     "    PRIMARY KEY (query_id, disk_id),\n" +
                     "    FOREIGN KEY (query_id) REFERENCES Query(id) ON DELETE CASCADE,\n" +
                     "    FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE\n" +
                     ");" +

                     "CREATE TABLE RamOnDisk\n" +
                     "(\n" +
                     "    ram_id integer,\n" +
                     "    disk_id integer,\n"
                     "    PRIMARY KEY (ram_id, disk_id),\n" +
                     "    FOREIGN KEY (ram_id) REFERENCES Ram(id) ON DELETE CASCADE,\n" +
                     "    FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE\n" +
                     ");" +

                     "CREATE VIEW ViewDiskAndQuery AS\n" +
                     "SELECT q.id AS query_id, q.disk_size_needed," +
                     "d.id AS disk_id, d.free_space" +
                     " FROM Query q, Disk d\n;" +

                     "CREATE VIEW ViewDiskOnQuery AS\n" +
                     "SELECT qod.query_id , qod.disk_id," +
                     "d.free_space, q.disk_size_needed" +
                     " FROM Query q, Disk d, QueryOnDisk as qod" +
                     "\tWHERE d.id = qod.disk_id AND q.id = qod.disk_id;\n" +
                     "COMMIT;")

    except DatabaseException.ConnectionInvalid as e:
        conn.rollbak()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollbak()
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollbak()
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollbak()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollbak()
    except Exception as e:
        conn.rollbak()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    pass


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN; DROP TABLE IF EXISTS Query, Disk, Ram, QueryOnDisk, RamOnDisk CASCADE;"
                     " DROP VIEW IF EXISTS ViewDiskAndQuery, ViewDiskOnQuery CASCADE; COMMIT;")
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        conn.rollback()
    except Exception as e:
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        id_param = query.getQueryID()
        purpose_param = query.getPurpose()
        size_param = query.getSize()
        q = sql.SQL("INSERT INTO Query(id, purpose, disk_size_needed) VALUES({id}, {purpose}, {disk_size})").format(id=sql.Literal(id_param), purpose=sql.Literal(purpose_param), disk_size=sql.Literal(size_param))
        conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def getQueryProfile(queryID: int) -> Query:
    conn = None
    rows_affected = 0
    result = ResultSet()

    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute("SELECT * "
                                            "FROM Query "
                                            "WHERE id = {0}").format(queryID)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_affected == 0:
        return BadQuery()
    result.next()
    return Query( result.getInt("id") , result.getString("purpose"), result.getInt("disk_size_needed"))


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        id_param = query.getQueryID()
        size_param = query.getSize()
        q = sql.SQL("BEGIN;"
                    "UPDATE Disk SET free_space = free_space - {size}"
                    "FROM QueryOnDisk As qod WHERE Disk.id = qod.disk_id AND qod.query_id = {se_ID};"
                    "DELETE FROM Query WHERE id = {ID};"
                    "COMMIT;").format(size=sql.Literal(size_param), se_ID=sql.Literal(id_param), ID=sql.Literal(id_param))
        conn.execute(q)
    except Exception as e:
        ret_val = ReturnValue.ERROR
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val

def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        id_param = query.getDiskID()
        company_param = query.getCompany()
        speed_param = query.getSpeed()
        free_space_param = query.getFreeSpace()
        cost_param = query.getCost()
        q = sql.SQL("INSERT INTO Disk(id, company, speed, free_space, cost) VALUES({id}, {company}, {speed}, {free_space}, {cost})").format(
            id=sql.Literal(id_param), company=sql.Literal(company_param), speed=sql.Literal(speed_param),
            free_space=sql.Literal(free_space_param), cost=sql.Literal(cost_param))
        conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    rows_affected = 0
    result = ResultSet()

    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute("SELECT * "
                                             "FROM Disk "
                                             "WHERE id = {0}").format(diskID)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_affected == 0:
        return BadQuery()
    result.next()
    return Disk(result.getInt("id"), result.getString("company"), result.getInt("speed"),
                 result.getInt("free_space"), result.getInt("cost"))


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        id_param = query.getQueryID()
        size_param = query.getSize()
        q = sql.SQL("DELETE FROM Disk WHERE id = {ID}").format(ID=sql.Literal(diskID))
        rows_affected, _ = conn.execute(q)
        if rows_affected == 0:
            ret_val = ReturnValue.NOT_EXISTS
        conn.commit()
    except Exception as e:
        ret_val = ReturnValue.ERROR
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        id_param = ram.getRamID()
        company_param = ram.getCompany()
        size_param = ram.getSize()
        q = sql.SQL("INSERT INTO Ram(id, company, size) VALUES({id}, {company}, {size})").format(
            id=sql.Literal(id_param), company=sql.Literal(company_param), size=sql.Literal(size_param))
        conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    rows_affected = 0
    result = ResultSet()

    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute("SELECT * "
                                             "FROM Ram "
                                             "WHERE id = {0}").format(ramID)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_affected == 0:
        return BadRam()
    result.next()
    return RAM(result.getInt("id"), result.getInt("size"), result.getString("company"))


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
        id_param = query.getQueryID()
        size_param = query.getSize()
        q = sql.SQL("DELETE FROM Ram WHERE id = {ID}").format(ID=sql.Literal(ramID))
        rows_affected, _ = conn.execute(q)
        if rows_affected == 0:
            ret_val = ReturnValue.NOT_EXISTS
        conn.commit()
    except Exception as e:
        ret_val = ReturnValue.ERROR
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query_id_param = query.getQueryID()
        purpose_param = query.getPurpose()
        size_param = query.getSize()
        q = sql.SQL("INSERT INTO Query(id, purpose, disk_size_needed) VALUES({id}, {purpose}, {disk_size})").format(
            id=sql.Literal(id_param), purpose=sql.Literal(purpose_param), disk_size=sql.Literal(size_param))

        disk_id_param = query.getDiskID()
        company_param = query.getCompany()
        speed_param = query.getSpeed()
        free_space_param = query.getFreeSpace()
        cost_param = query.getCost()
        q = sql.SQL(
            "BEGIN;"
            "INSERT INTO Disk(disk_id, company, speed, free_space, cost) VALUES({disk_id}, {company}, {speed}, {free_space}, {cost});"
            "INSERT INTO Query(query_id, purpose, disk_size_needed) VALUES({query_id}, {purpose}, {disk_size});"
            "COMMIT;").format(
            disk_id=sql.Literal(disk_id_param), company=sql.Literal(company_param), speed=sql.Literal(speed_param),
            free_space=sql.Literal(free_space_param), cost=sql.Literal(cost_param),
            query_id=sql.Literal(query_id_param), purpose=sql.Literal(purpose_param), disk_size=sql.Literal(size_param))
        conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []
