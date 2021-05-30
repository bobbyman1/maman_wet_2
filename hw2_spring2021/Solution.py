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
        conn.execute("CREATE TABLE Users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        conn.execute("CREATE TABLE Query\n" +
                     "(\n" +
                     "    id integer,\n" +
                     "    purpose text NOT NULL ,\n" +
                     "    disk_size_needed integer NOT NULL ,\n" +
                     "    PRIMARY KEY (id),\n" +
                     "    CHECK (id > 0),\n" +
                     "    CHECK (disk_size_needed >= 0)\n" +
                     ")")

        conn.execute("CREATE TABLE Disk\n" +
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
                     ")")

        conn.execute("CREATE TABLE Ram\n" +
                     "(\n" +
                     "    id integer,\n" +
                     "    size integer NOT NULL ,\n" +
                     "    company text NOT NULL ,\n" +
                     "    PRIMARY KEY (id),\n" +
                     "    CHECK (id > 0),\n" +
                     "    CHECK (size > 0)\n" +
                     ")")

        conn.execute("CREATE TABLE QueryOnDisk\n" +
                     "(\n" +
                     "    query_id integer,\n" +
                     "    disk_id integer,\n"
                     "    PRIMARY KEY (query_id, disk_id),\n" +
                     "    FOREIGN KEY (query_id) REFERENCES Query(id) ON DELETE CASCADE,\n" +
                     "    FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE\n" +
                     ")")

        conn.execute("CREATE TABLE RamOnDisk\n" +
                     "(\n" +
                     "    ram_id integer,\n" +
                     "    disk_id integer,\n"
                     "    PRIMARY KEY (ram_id, disk_id),\n" +
                     "    FOREIGN KEY (ram_id) REFERENCES Ram(id) ON DELETE CASCADE,\n" +
                     "    FOREIGN KEY (disk_id) REFERENCES Disk(id) ON DELETE CASCADE\n" +
                     ")")

        conn.execute("CREATE VIEW ViewDiskAndQuery AS\n" +
                     "SELECT q.id AS query_id, q.disk_size_needed," +
                     "d.id AS disk_id, d.free_space" +
                     " FROM Query q, Disk d\n")

        conn.execute("CREATE VIEW ViewDiskOnQuery AS\n" +
                     "SELECT qod.query_id , qod.disk_id," +
                     "d.free_space, q.disk_size_needed" +
                     " FROM Query q, Disk d, QueryOnDisk as qod" +
                     "\tWHERE d.id = qod.disk_id AND q.id = qod.disk_id;\n")

        conn.commit()
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
        conn.execute("DROP TABLE IF EXISTS Users,Query, Disk, Ram, QueryOnDisk, RamOnDisk CASCADE")
        conn.execute("DROP VIEW IF EXISTS ViewDiskAndQuery, ViewDiskOnQuery CASCADE")
        conn.commit()
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
    return Query()


def deleteQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
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
