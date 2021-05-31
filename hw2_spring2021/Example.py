import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql
from typing import List

def errorHandler(e):
    ret_val = ReturnValue.ERROR
    if isinstance(e,DatabaseException.NOT_NULL_VIOLATION):
        ret_val = ReturnValue.BAD_PARAMS
    elif isinstance(e,DatabaseException.CHECK_VIOLATION):
        ret_val = ReturnValue.BAD_PARAMS
    elif isinstance(e,DatabaseException.UNIQUE_VIOLATION):
        ret_val = ReturnValue.ALREADY_EXISTS
    return ret_val
def dropTable() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN; DROP TABLE IF EXISTS Users,Query, Disk, Ram, QueryOnDisk, RamOnDisk CASCADE;"
                     " DROP VIEW IF EXISTS ViewDiskAndQuery, ViewDiskOnQuery CASCADE; COMMIT;")
        #conn.execute("")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def createTable() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
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
                     "d.id AS disk_id, d.free_space AS free_space" +
                     " FROM Query q, Disk d\n;" +

                    "CREATE VIEW ViewDiskOnQuery AS\n" +
                     "SELECT qod.query_id AS query_id, qod.disk_id AS disk_id," +
                     "d.free_space AS free_space, q.disk_size_needed AS disk_size_needed" +
                     " FROM Query q, Disk d, QueryOnDisk as qod" +
                     "\tWHERE d.id = qod.disk_id AND q.id = qod.query_id;\n" +
                    "COMMIT;")

        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def getUsers(printSchema) -> ResultSet:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Users", printSchema=printSchema)
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result


def addUser(ID: int, name: str) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Users(id, name) VALUES({id}, {username})").format(id=sql.Literal(ID),
                                                                                       username=sql.Literal(name))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK
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
        return BadDisk()
    result.next()
    return Disk(result.getInt("id"), result.getString("company"), result.getInt("speed"),
                result.getInt("free_space"), result.getInt("cost"))


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
        q = sql.SQL("INSERT INTO Ram(id, size, company) VALUES({id}, {size}, {company})").format(
            id=sql.Literal(id_param), size=sql.Literal(size_param) , company=sql.Literal(company_param) )
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
        disk_id_param = disk.getDiskID()
        company_param = disk.getCompany()
        speed_param = disk.getSpeed()
        free_space_param = disk.getFreeSpace()
        cost_param = disk.getCost()
        q = sql.SQL(
            "BEGIN;"
            "INSERT INTO Disk(id, maunfacturing_company, speed, free_space, cost_per_byte) VALUES({disk_id}, {company},"
            "{speed}, {free_space}, {cost});"
            "INSERT INTO Query(id, purpose, disk_size_needed) VALUES({query_id}, {purpose}, {disk_size});"
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
    try:
        conn = Connector.DBConnector()
        query_id_param = query.getQueryID()
        query_purpose_param = query.getPurpose()
        query_size_param = query.getSize()
        q = sql.SQL(
            "BEGIN;"
            "INSERT INTO QueryOnDisk(query_id, disk_id) VALUES({query_id}, {disk_id});"
            "UPDATE Disk SET free_space=free_space-({size}) WHERE id=({disk_id});"
            "COMMIT;").format(query_id=sql.Literal(query_id_param), disk_id=sql.Literal(diskID),size=sql.Literal(query_size_param))
        conn.execute(q)
        conn.commit()
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
            "DELETE FROM QueryOnDisk WHERE query_id=({query_id}) AND disk_id= ({disk_id});"
            "UPDATE Disk SET free_space=free_space+({size}) WHERE id=({disk_id});"
            "COMMIT;").format(query_id=sql.Literal(query_id_param), disk_id=sql.Literal(diskID),size=sql.Literal(query_size_param))
        conn.execute(q)

    except Exception as e:
        ret_val = errorHandler(e)
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
                "INSERT INTO RamOnDisk(ram_id, disk_id) VALUES({ram_id}, {disk_id})").format(ram_id=sql.Literal(ramID), disk_id=sql.Literal(diskID))
        conn.execute(q)
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
            "BEGIN;"
            "DELETE FROM RamOnDisk WHERE ram_id={ram_id} AND disk_id={disk_id};"
            "COMMIT;").format(ram_id=sql.Literal(ramID), disk_id=sql.Literal(diskID))
        conn.execute(q)
    except Exception as e:
        ret_val = errorHandler(e)
        conn.rollback()
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return ret_val

def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    rows_effected, result = 0, ResultSet()
    rows_effected2, result2 = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT AVG(disk_size_needed) FROM ViewDiskOnQuery WHERE disk_id={disk_id}").format(disk_id=sql.Literal(diskID))
        rows_effected, result=conn.execute(q,printSchema=False)
        conn.commit()
        conn.close()
        return result[0]['avg']

    except ZeroDivisionError:
        conn.rollback()
        conn.close()
        return 0
    except Exception as e:
        conn.rollback()
        conn.close()
        return -1
    conn.close()
    return 0




def diskTotalRAM(diskID: int) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    rows_effected2, result2 = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT SUM(r.size) FROM Ram AS r, RamOnDisk AS rod WHERE r.id=rod.ram_id AND rod.disk_id={disk_id}").format(disk_id=sql.Literal(diskID))
        rows_effected, result=conn.execute(q,printSchema=False)
        conn.commit()
        conn.close()
        return result[0]['sum']

    except ZeroDivisionError:
        conn.rollback()
        conn.close()
        return 0
    except Exception as e:
        conn.rollback()
        conn.close()
        return -1
    conn.close()
    return 0

def getCostForPurpose(purpose: str) -> int:
    conn = None
    rows_effected, result = 0, ResultSet()
    rows_effected2, result2 = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT SUM(d.cost_per_byte*vdoq.disk_size_needed) FROM ViewDiskOnQuery AS vdoq, Disk AS d, Query AS q WHERE q.purpose={purpose} AND q.id=vdoq.query_id AND d.id=vdoq.disk_id").format(purpose=sql.Literal(purpose))
        rows_effected, result=conn.execute(q,printSchema=False)
        return result[0]['sum']

    except ZeroDivisionError:
        conn.rollback()
        conn.close()
        return 0
    except Exception as e:
        conn.rollback()
        conn.close()
        return -1
    conn.close()
    return 0

def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    retList=[]
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT q.id FROM Query AS q, Disk AS d WHERE d.id={disk_id}  AND d.free_space>=q.disk_size_needed ORDER BY q.id ASC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_effected, result=conn.execute(q,printSchema=False)
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
    retList=[]
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        q = sql.SQL("SELECT q.id FROM Query AS q, Disk AS d WHERE d.id=1  AND d.free_space>=q.disk_size_needed AND q.disk_size_needed<=(SELECT SUM(r.size) FROM Ram AS r, RamOnDisk AS rod WHERE r.id=rod.ram_id AND rod.disk_id=1) ORDER BY q.id ASC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_effected, result=conn.execute(q,printSchema=False)
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


def deleteUser(ID: int, persistent: bool = True) -> int:
    conn = None
    rows_effected = 0
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Users WHERE id={0}").format(sql.Literal(ID))
        rows_effected, _ = conn.execute(query)
        if persistent:
            conn.commit()
            # the deletion stands across all connections
        else:
            conn.rollback()
            # now the deletion does not occur even within this connection
            # not rolling back means the deletion stands within the connection only
            # useful in case of a transaction consists of two queries which we don't want to happen if the second query fails
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return rows_effected


if __name__ == '__main__':
    print("0. Creating all tables")
    createTable()
    print("1. Add user with ID 1 and name Roei")
    addUser(1, 'Roei')
    print("2. Add user with ID 2 and name Noa")
    addUser(2, 'Noa')
    print('3. Can reinsert the same row since no commit was done')
    addUser(2, 'Noa')
    print("4. Printing all users")
    users = getUsers(printSchema=True)  # will cause printing the users, because printSchema=true in getUsers()
    print('5. Printing user in the second row')
    print(users[1]['id'], users[1]['name'])
    print("6. Printing all IDs")
    for index in range(users.size()):
        print(users[index]['ID'])
    print("7. Delete user with ID 1")
    deleteUser(1)
    print("8. Printing all users")
    users = getUsers(printSchema=False)  # will not cause printing the users, because printSchema=false in getUsers()
    # print users
    for index in range(users.size()):  # for each user
        current_row = users[index]  # get the row
        for col in current_row:  # iterate over the columns
            print(str(col) + "=" + str(current_row[col]))
    print("9. Delete user with ID 2, but do not commit, hence it is valid only within the connection")
    deleteUser(2, False)
    print("10. Printing all users - no change")
    users = getUsers(printSchema=False)  # will not cause printing the users, because printSchema=false in getUsers()
    # print users
    for index in range(users.size()):  # for each user
        current_row = users[index]  # get the row
        for col in current_row:  # iterate over the columns
            print(str(col) + "=" + str(current_row[col]))
    print("10.5. testing add query")
    query = Query(1, "test", 10)
    query2 = Query(2, "test", 11)
    query3 = Query(3, "test", 12)
    query4 = Query(4, "test", -1)
    query5 = Query(5, "test", 34)
    query6 = Query(6, "test", 21)
    addQuery(query)
    addQuery(query2)
    addQuery(query3)
    addQuery(query4)
    addQuery(query5)
    addQuery(query6)
    disky = Disk(1,"Siemens",1,66,11)
    a=addDisk(disky)
    b=addQueryToDisk(query, 1)
    b1=addQueryToDisk(query2, 1)
    b2=addQueryToDisk(query3, 1)
    c=averageSizeQueriesOnDisk(1)
    #d=removeQueryFromDisk(query,1)
    ram1=RAM(1,"yay",1)
    ram2=RAM(2,"yay",1)
    addRAM(ram1)
    addRAM(ram2)
    e=addRAMToDisk(1,1)
    f=addRAMToDisk(2,1)
    g=diskTotalRAM(1)
    h=getCostForPurpose("test")
    i=getQueriesCanBeAddedToDisk(1)
    print("11. Dropping all tables - empty database")
    dropTable()
