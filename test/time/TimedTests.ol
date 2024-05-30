from database import Database, ConnectionInfo
from console import Console
from time import Time
from file import File

from ..assertions import Assertions

interface TimedTestsInterface {
    RequestResponse:

    /// @BeforeAll
    setup_connection(void)(void),

    /// @BeforeEach
    populate_tables(void)(void),

    /// @Test
    query_time_500_entries(void)(void) throws AssertionError,

    
    /// @Test
    update_time_500_entries(void)(void) throws AssertionError,

    
    /// @Test
    delete_time_500_entries(void)(void) throws AssertionError,

    
    /// @Test
    insert_time_500_entries(void)(void) throws AssertionError,
    
    
    /// @Test
    query_time_500_entries_transaction(void)(void) throws AssertionError,

    
    /// @Test
    update_time_500_entries_transaction(void)(void) throws AssertionError,

    
    /// @Test
    delete_time_500_entries_transaction(void)(void) throws AssertionError,

    
    /// @Test
    insert_time_500_entries_transaction(void)(void) throws AssertionError,

    
    example_no_transaction(void)(void),

    
    example_transaction(void)(void),

    
    ten_transactions_simultaniously_old(void)(void),

    
    ten_transactions_simultaniously_new(void)(void),

    /// @AfterAll
    write_results(void)(void)
}

type TestParams{
    username: string
    password: string
    database: string
    driver: string
    host: string
}

service TimedTests(p: TestParams){
    execution: sequential
    inputPort Input {
        Location: "local"
        Interfaces: TimedTestsInterface
    }

    embed Assertions as Assertions
    embed Console as Console
    embed Database as Database
    embed Time as Time
    embed File as File

    main{
        [setup_connection()(){
            global.results = ""
            println@Console("Connecting to db: " + p.database)()
            connect@Database(p)()
            println@Console("Connected to db: " + p.database)()
            update@Database("CREATE TABLE IF NOT EXISTS testTable(id INTEGER, testString VARCHAR(50));")()
            if (p.driver == "hsqldb_embedded"){
                update@Database("SET DATABASE TRANSACTION CONTROL MVCC;")()
            }
            close@Database()()
        }]  

        [populate_tables()(){
            connect@Database(p)()
            update@Database("DELETE FROM testTable WHERE true;")()
            i = 0;
            while (i < 500){
                update@Database("INSERT INTO testTable(id, testString) VALUES ( " + i + ", 'testUser');")()
                i++
            }
            println@Console("Database Populated")()
        }]

        [write_results()(){
            close@Database()()

            readFile@File({
                .filename = "results/results.csv"
            })(results)
            with (writeFileRequest){
                .format = "text"
                .filename = "results/results.csv"
                .content << (results + global.results)
            }

            writeFile@File(writeFileRequest)()
        }]

        [query_time_500_entries()(){
            global.results = global.results + "query_time_500_entries"
            getCurrentTimeMillis@Time()(time)
            
            i = 0
            while (i < 500){
                query@Database("Select * from testTable")(res)
                i++
            }
            
            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time)
            global.results = global.results + "\n"

        }]

        [insert_time_500_entries()(){
            global.results = global.results + "insert_time_500_entries"
            getCurrentTimeMillis@Time()(time)

            i = 0
            while (i < 500){
                update@Database("INSERT INTO testTable(id, testString) VALUES ( " + i + ", 'testUser');")()
                i++
            }

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"

        }]

        [update_time_500_entries()(){
            global.results = global.results + "update_time_500_entries"

            getCurrentTimeMillis@Time()(time)

            i = 0
            while (i < 500){
                update@Database("UPDATE testTable SET teststring = 'UpdatedUsername " + i + "'  where id = " + i + ";")(res)
                i++
            }
            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [delete_time_500_entries()(){
            global.results = global.results + "delete_time_500_entries"

            getCurrentTimeMillis@Time()(time)

            i = 0
            while (i < 500){
                update@Database("DELETE FROM testTable WHERE id = " + i + ";")(res)
                i++
            }

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [insert_time_500_entries_transaction()(){
            global.results = global.results + "insert_time_500_entries_transaction"

            getCurrentTimeMillis@Time()(time)

            i = 0
            beginTx@Database()(txHandle)
            while (i < 500){
                update@Database({
                    txHandle = txHandle
                    update="INSERT INTO testTable(id, testString) VALUES ( " + i + ", 'testUser');"
                    })()
                i++
            }
            commitTx@Database(txHandle)()

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"

        }]

        [query_time_500_entries_transaction()(){
            global.results = global.results + "query_time_500_entries_transaction"
            getCurrentTimeMillis@Time()(time)
            
            i = 0
            beginTx@Database()(txHandle)
            while (i < 500){
                query@Database( {txHandle=txHandle
                query="Select * from testTable"})(res)
                i++
            }
            commitTx@Database(txHandle)()
            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time)
            global.results = global.results + "\n"

        }]

        [update_time_500_entries_transaction()(){
            global.results = global.results + "update_time_500_entries_transaction"

            getCurrentTimeMillis@Time()(time)

            beginTx@Database()(txHandle)
            i = 0
            while (i < 500){
                update@Database({txHandle=txHandle
                update="UPDATE testTable SET teststring = 'UpdatedUsername " + i + "'  where id = " + i + ";"})(res)
                i++
            }
            commitTx@Database(txHandle)()
            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [delete_time_500_entries_transaction()(){
            global.results = global.results + "delete_time_500_entries_transaction"

            getCurrentTimeMillis@Time()(time)
            beginTx@Database()(txHandle)

            i = 0
            while (i < 500){
                update@Database({txHandle=txHandle
                update="DELETE FROM testTable WHERE id = " + i + ";"})(res)
                i++
            }
            commitTx@Database(txHandle)()

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [example_no_transaction()(){
            global.results = global.results + "query_update_no_transaction"

            getCurrentTimeMillis@Time()(time)

            i = 0
            while (i < 500){
                query@Database("SELECT * FROM testTable WHERE id = 500")()
                update@Database("UPDATE testTable SET testString = 'SomenewName' WHERE id = 500;")()
                i++
            }

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [example_transaction()(){
            global.results = global.results + "query_update_transaction"

            getCurrentTimeMillis@Time()(time)

            beginTx@Database()(txHandle)
            query@Database({
                .query = "SELECT * FROM testTable WHERE id = 500"
                .txHandle = txHandle
            })()

            update@Database({
                .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 500;"
                .txHandle = txHandle
            })()

            commitTx@Database(txHandle)()

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [ten_transactions_simultaniously_old()(){
            global.results = global.results + "executeTransaction_ten_transactions"
            getCurrentTimeMillis@Time()(time)

            {
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 500;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 501;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 502;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 503;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 504;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 505;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 506;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 507;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 508;"})()
                |
                executeTransaction@Database({.statement[0] = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 509;"})()
            }

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]

        [ten_transactions_simultaniously_new()(){
            global.results = global.results + "beginTx_ten_transactions"
            getCurrentTimeMillis@Time()(time)

            {
                beginTx@Database()(tx0); 
                update@Database(
                    {.txHandle = tx0; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 500;"}
                )();  
                commitTx@Database(tx0)()
                |
                beginTx@Database()(tx1)
                update@Database(
                        {.txHandle = tx1; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 501;"}
                    )();  
                commitTx@Database(tx1)() 
                |
                beginTx@Database()(tx2) 
                update@Database(
                        {.txHandle = tx2; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 502;"}
                    )();  
                commitTx@Database(tx2)()
                |
                beginTx@Database()(tx3)
                update@Database(
                        {.txHandle = tx3; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 503;"}
                    )();  
                commitTx@Database(tx3)()
                |
                beginTx@Database()(tx4)
                update@Database(
                        {.txHandle = tx4; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 504;"}
                    )();  
                commitTx@Database(tx4)()
                |
                beginTx@Database()(tx5) 
                update@Database(
                        {.txHandle = tx5; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 505;"}
                    )();  
                commitTx@Database(tx5)()
                |
                beginTx@Database()(tx6) 
                update@Database(
                        {.txHandle = tx6; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 506;"}
                    )();  
                commitTx@Database(tx6)()
                |
                beginTx@Database()(tx7)
                update@Database(
                        {.txHandle = tx7; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 507;"}
                    )();  
                commitTx@Database(tx7)()
                |
                beginTx@Database()(tx8) 
                update@Database(
                        {.txHandle = tx8; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 508;"}
                    )();  
                commitTx@Database(tx8)()
                |
                beginTx@Database()(tx9)
                update@Database(
                        {.txHandle = tx9; .update = "UPDATE testTable SET testString = 'SomenewName' WHERE id = 509;"}
                    )();  
                commitTx@Database(tx9)()
            }

            getCurrentTimeMillis@Time()(time2)
            global.results = global.results + " " + (time2 - time) + "\n"
        }]
    }
}