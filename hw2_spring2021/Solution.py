from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.DBConnector import ResultSet
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def errorHandler(e):
    val = ReturnValue.ERROR
    if isinstance(e,DatabaseException.NOT_NULL_VIOLATION):
        val = ReturnValue.BAD_PARAMS
    elif isinstance(e,DatabaseException.CHECK_VIOLATION):
        val = ReturnValue.BAD_PARAMS
    elif isinstance(e,DatabaseException.UNIQUE_VIOLATION):
        val = ReturnValue.ALREADY_EXISTS
    elif isinstance(e, DatabaseException.FOREIGN_KEY_VIOLATION):
        val = ReturnValue.NOT_EXISTS
    return val




def createTables() -> None:
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
                     "SELECT q.id AS query_id, q.disk_size_needed AS disk_size_needed," +
                     "d.id AS disk_id, d.free_space AS free_space, d.speed AS speed" +
                     " FROM Query q, Disk d\n;" +

                     "CREATE VIEW ViewDiskOnQuery AS\n" +
                     "SELECT qod.query_id AS query_id, qod.disk_id AS disk_id," +
                     "d.free_space AS free_space, q.disk_size_needed AS disk_size_needed" +
                     " FROM Query q, Disk d, QueryOnDisk as qod" +
                     "\tWHERE d.id = qod.disk_id AND q.id = qod.query_id;\n" +
                     "COMMIT;")

    except Exception as e:
        conn.rollback()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM Query CASCADE ;"
                     "DELETE FROM Disk CASCADE ;"
                     "DELETE FROM Ram CASCADE ;"
                     "DELETE FROM QueryOnDisk CASCADE ;"
                     "DELETE FROM RamOnDisk CASCADE ;"
                     "COMMIT;")

    except Exception as e:
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN; DROP TABLE IF EXISTS Query, Disk, Ram, QueryOnDisk, RamOnDisk CASCADE;"
                     " DROP VIEW IF EXISTS ViewDiskAndQuery, ViewDiskOnQuery CASCADE; COMMIT;")

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
        q = sql.SQL("SELECT *  FROM Query WHERE id = {id}").format(id=sql.Literal(queryID))
        rows_affected, result = conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_affected == 0:
        return Query.badQuery()
    return Query(result[0]["id"], result[0]["purpose"], result[0]["disk_size_needed"])


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        id_param = query.getQueryID()
        size_param = query.getSize()
        q = sql.SQL("BEGIN;"
                    "UPDATE Disk SET free_space = free_space + {size}"
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
        id_param = disk.getDiskID()
        company_param = disk.getCompany()
        speed_param = disk.getSpeed()
        free_space_param = disk.getFreeSpace()
        cost_param = disk.getCost()
        q = sql.SQL("INSERT INTO Disk(id, manufacturing_company, speed, free_space, cost_per_byte) VALUES({id}, {company}, {speed}, {free_space}, {cost})").format(
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
        q = sql.SQL("SELECT * FROM Disk WHERE id = {disk_id}").format(disk_id=sql.Literal(diskID))
        rows_affected, result = conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_affected == 0:
        return Disk.badDisk()
    return Disk(result[0]["id"], result[0]["manufacturing_company"], result[0]["speed"], result[0]["free_space"],
                result[0]["cost_per_byte"])


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
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
        q = sql.SQL("INSERT INTO Ram(id, size, company) VALUES({id}, {size},  {company})").format(
            id=sql.Literal(id_param), size=sql.Literal(size_param), company=sql.Literal(company_param))
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
        q = sql.SQL("SELECT *  FROM Ram  WHERE id = {id}").format(id=sql.Literal(ramID))
        rows_affected, result = conn.execute(q)
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    if rows_affected == 0:
        return RAM.badRAM()
    return RAM(result[0]["id"], result[0]["company"], result[0]["size"])


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    rows_affected = 0
    try:
        conn = Connector.DBConnector()
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
        disk_id_param = disk.getDiskID()
        company_param = disk.getCompany()
        speed_param = disk.getSpeed()
        free_space_param = disk.getFreeSpace()
        cost_param = disk.getCost()
        q = sql.SQL(
            "BEGIN;" +
            "INSERT INTO Disk(id, manufacturing_company, speed, free_space, cost_per_byte) VALUES({disk_id}, {company},"
            +"{speed}, {free_space}, {cost});" +
            "INSERT INTO Query(id, purpose, disk_size_needed) VALUES({query_id}, {purpose}, {disk_size});" +
            "COMMIT;").format(
            disk_id=sql.Literal(disk_id_param), company=sql.Literal(company_param), speed=sql.Literal(speed_param),
            free_space=sql.Literal(free_space_param), cost=sql.Literal(cost_param),
            query_id=sql.Literal(query_id_param), purpose=sql.Literal(purpose_param), disk_size=sql.Literal(size_param))
        conn.execute(q)
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val

def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query_id_param = query.getQueryID()
        query_purpose_param = query.getPurpose()
        query_size_param = query.getSize()
        q = sql.SQL(
            "BEGIN;"
            "UPDATE Disk SET free_space=free_space-({size}) FROM Query AS q WHERE id = {disk_id} AND {query_id} IN (q.id);"
            "INSERT INTO QueryOnDisk(query_id, disk_id) VALUES({query_id}, {disk_id});"
            "COMMIT;").format(query_id=sql.Literal(query_id_param), disk_id=sql.Literal(diskID),
                              size=sql.Literal(query_size_param))
        rows_effected, result =conn.execute(q)
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
    # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query_id_param = query.getQueryID()
        query_purpose_param = query.getPurpose()
        query_size_param = query.getSize()
        q = sql.SQL(
            "BEGIN;"            
            "UPDATE Disk SET free_space=free_space+({size}) FROM QueryOnDisk AS qod WHERE id=({disk_id}) AND qod.query_id={query_id} AND qod.disk_id={disk_id};"
            "DELETE FROM QueryOnDisk WHERE query_id=({query_id}) AND disk_id= ({disk_id});"
            "COMMIT;").format(query_id=sql.Literal(query_id_param), disk_id=sql.Literal(diskID),
                              size=sql.Literal(query_size_param))
        conn.execute(q)

    except Exception as e:
        ret_val = ReturnValue.ERROR
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val

def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "INSERT INTO RamOnDisk(ram_id, disk_id) VALUES({ram_id}, {disk_id})").format(ram_id=sql.Literal(ramID),
                                                                                         disk_id=sql.Literal(diskID))
        rows_effected, result =conn.execute(q)
        if rows_effected==0:
            ret_val=ReturnValue.NOT_EXISTS
        # TODO - indent? commit?
        conn.commit()
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
    # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    ret_val = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "DELETE FROM RamOnDisk WHERE ram_id={ram_id} AND disk_id={disk_id};"
            ).format(ram_id=sql.Literal(ramID), disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(q)
        if rows_effected == 0:
            ret_val = ReturnValue.NOT_EXISTS
    except Exception as e:
        ret_val = ReturnValue.ERROR
        conn.rollback()
    finally:
    # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val


def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT AVG(disk_size_needed) FROM ViewDiskOnQuery WHERE disk_id={disk_id}").format(
            disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(q, printSchema=False)
        conn.commit()
        conn.close()
        if rows_effected == 0:
            return 0

        return result[0]['avg']

    except ZeroDivisionError:
        conn.rollback()
        conn.close()
        return 0
    except Exception as e:
        conn.rollback()
        conn.close()
        return -1
    return 0


def diskTotalRAM(diskID: int) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    rows_effected2, result2 = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT SUM(r.size) FROM Ram AS r, RamOnDisk AS rod WHERE r.id=rod.ram_id AND rod.disk_id={disk_id}").format(
            disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(q, printSchema=False)
        conn.commit()
        conn.close()
        if rows_effected==0:
            return 0

        return result[0]['sum']

    except Exception as e:
        conn.rollback()
        conn.close()
        return -1
    return 0


def getCostForPurpose(purpose: str) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    rows_effected2, result2 = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT SUM(d.cost_per_byte*vdoq.disk_size_needed) FROM ViewDiskOnQuery AS vdoq, Disk AS d, Query AS q WHERE q.purpose={purpose} AND q.id=vdoq.query_id AND d.id=vdoq.disk_id").format(
            purpose=sql.Literal(purpose))
        rows_effected, result = conn.execute(q, printSchema=False)
        conn.commit()
        conn.close()
        if rows_effected==0:
            return 0

        return result[0]['sum']

    except Exception as e:
        conn.rollback()
        conn.close()
        return -1
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    retList = []
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT q.id FROM Query AS q, Disk AS d WHERE d.id={disk_id}  AND d.free_space>=q.disk_size_needed ORDER BY q.id ASC LIMIT 5").format(
            disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(q, printSchema=False)
        # print users
        for index in range(result.size()):  # for each user
            current_row = result[index]  # get the row
            for col in current_row:  # iterate over the columns
                retList.append(str(current_row[col]))
        conn.commit()
        conn.close()
        return retList

    except ZeroDivisionError:
        conn.rollback()
    except Exception as e:
        conn.rollback()
    conn.close()
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    retList = []
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT q.id FROM Query AS q, Disk AS d WHERE d.id=1  AND d.free_space>=q.disk_size_needed AND q.disk_size_needed<=(SELECT SUM(r.size) FROM Ram AS r, RamOnDisk AS rod WHERE r.id=rod.ram_id AND rod.disk_id=1) ORDER BY q.id ASC LIMIT 5").format(
            disk_id=sql.Literal(diskID))
        rows_effected, result = conn.execute(q, printSchema=False)
        # print users
        for index in range(result.size()):  # for each user
            current_row = result[index]  # get the row
            for col in current_row:  # iterate over the columns
                retList.append(str(current_row[col]))
        conn.commit()
        conn.close()
        return retList

    except ZeroDivisionError:
        conn.rollback()
    except Exception as e:
        conn.rollback()
    conn.close()
    return []



def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT count(*) WHERE 0=(SELECT COUNT(r.id) AS numOfID FROM RamOnDisk AS rod, Disk AS d, Ram AS r WHERE rod.disk_id={disk_id} AND r.id=rod.ram_id AND d.id={disk_id} AND d.manufacturing_company<>r.company)").format(disk_id=sql.Literal(diskID))
        rows_effected, result=conn.execute(q,printSchema=False)
        return result[0]['count']

    except Exception as e:
        conn.rollback()
        conn.close()
        return 0
    conn.close()
    return 0

    return True
def getConflictingDisks() -> List[int]:
    conn = None
    retList = []
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT DISTINCT q1.disk_id FROM QueryOnDisk AS q1, QueryOnDisk AS q2 "
            "WHERE q1.query_id = q2.query_id AND q1.disk_id <> q2.disk_id")
        rows_effected, result = conn.execute(q, printSchema=False)
        # print users
        for index in range(result.size()):  # for each user
            current_id = result[index]["q1.disk_id"]  # get the row
            retList.append(current_id)
        conn.commit()
        conn.close()
        return retList
    except Exception as e:
        conn.rollback()
    conn.close()
    return []


def mostAvailableDisks() -> List[int]:
    conn = None
    retList = []
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        num_elements = 5
        q = sql.SQL(
            "SELECT disk_id FROM ViewDiskAndQuery"
            "WHERE free_space >= disk_size_needed"
            "GROUP BY disk_id, speed"
            "ORDER BY COUNT(query_id) DESC, speed DESC, disk_id ASC")
        rows_effected, result = conn.execute(q, printSchema=False)
        # print users
        if rows_effected<num_elements:
            num_elements = rows_effected
        for index in range(num_elements):  # for each user
            disk_id = result[index]["disk_id"]  # get the row
            retList.append(disk_id)
        conn.commit()
        conn.close()
        return retList
    except Exception as e:
        conn.rollback()
    conn.close()
    return []


def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    retList = []
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL(
            "SELECT DISTINCT(q.id) FROM Query AS q,QueryOnDisk AS qod, (SELECT C.query_id AS id, COUNT(*) "
            "FROM (SELECT qod.query_id AS query_id ,qod.disk_id AS disk_id FROM QueryOnDisk AS qod"
            "WHERE qod.query_id<>{ID} AND qod.disk_id IN "
            "(SELECT qod.disk_id FROM QueryOnDisk AS qod WHERE qod.query_id={ID})) AS C "
            "GROUP BY C.query_id"
            "HAVING COUNT(*)  >= (SELECT COUNT(qod.disk_id)  FROM QueryOnDisk AS qod WHERE qod.query_id={ID} )/2.0 "
            "ORDER BY C.query_id ASC LIMIT 10) AS q2"
            "WHERE q.id<>1 AND(q.id=q2.id OR 0 = (SELECT COUNT(qod.disk_id) "
            "FROM QueryOnDisk AS qod"
            "WHERE qod.query_id={ID}))"
            "ORDER BY q.id ASC LIMIT 10").format(ID = sql.Literal(queryID))
        rows_effected, result = conn.execute(q, printSchema=False)

        for index in range(result.size()):  # for each user
            query_id = result[index]["q.id"]  # get the row
            retList.append(query_id)
        conn.commit()
        conn.close()
        return retList
    except Exception as e:
        conn.rollback()
    conn.close()
    return []
