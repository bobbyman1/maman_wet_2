import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from Business.Query import Query
from psycopg2 import sql


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
                     "SELECT q.id AS query_id, q.disk_size_needed," +
                     "d.id AS disk_id, d.free_space" +
                     " FROM Query q, Disk d\n;" +

                    "CREATE VIEW ViewDiskOnQuery AS\n" +
                     "SELECT qod.query_id , qod.disk_id," +
                     "d.free_space, q.disk_size_needed" +
                     " FROM Query q, Disk d, QueryOnDisk as qod" +
                     "\tWHERE d.id = qod.disk_id AND q.id = qod.disk_id;\n" +
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
    try:
        conn = Connector.DBConnector()
        id_param = query.getQueryID()
        purpose_param = query.getPurpose()
        size_param = query.getSize()
        q = sql.SQL("INSERT INTO Query(id, purpose, disk_size_needed) VALUES({id}, {purpose}, {disk_size})").format(id=sql.Literal(id_param), purpose=sql.Literal(purpose_param), disk_size=sql.Literal(size_param))
        conn.execute(q)
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
    return ReturnValue.OK



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
    addQuery(query)
    print("11. Dropping all tables - empty database")
    dropTable()
