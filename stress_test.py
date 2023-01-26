import time
import random
from connection_pool import ConnectionPool, PoolError
import threading


tables = 0
db_lock = threading.Lock()


def create_table(connection):
    global tables
    cursor = connection.cursor()
    table_name = "test%s" % (tables,)
    db_lock.acquire()
    cursor.execute("SELECT to_regclass(%s);", (table_name,))
    if cursor.fetchone()[0] is not None:
        print(f"Table {table_name} already exists.")
    else:
        cursor.execute("CREATE TABLE %s (id serial primary key, num integer, data text);" % (table_name,))
        print(f"Table {table_name} created successfully")
        tables += 1
    connection.commit()
    db_lock.release()

def insert_into_table(connection):
    global tables
    cursor = connection.cursor()
    db_lock.acquire()
    cursor.execute("INSERT INTO test%s (num, data) Values (%s, %s)", (random.randint(0, tables), random.randint(0, 100), "abcdef"))
    print(f"Table{tables} inserted successfully")
    connection.commit()
    db_lock.release()
    

requests = ["Select * from test%s;", "Select num from test%s;", "Select data from test%s;",\
                "Select * from test%s where data = 'abcdef'", "Select num from test%s where num > 10"]
def send_select(connection, request):
    global tables
    cursor = connection.cursor()
    db_lock.acquire()
    cursor.execute(request, (random.randint(0,tables), ))
    db_lock.release()
    result = cursor.fetchall()
    print(result)
    


connection_pool = ConnectionPool("postgres://postgres:superuser@localhost:5432/connection_pool")
elapsed_time = 0
start_time = time.time()



while elapsed_time <= 600:    # 600 seconds == 10 minutes
    elapsed_time = time.time() - start_time
    for _ in range(random.randint(1, 10)):
        thread = threading.Thread(target=connection_pool.get_connection)
        connection_pool.thread_list.append(thread)

    for conn in connection_pool.used:
        if connection_pool.used.index(conn) % 3 == 0:
            thread = threading.Thread(target=create_table, args=(conn, ))
            connection_pool.thread_list.append(thread)
        elif connection_pool.used.index(conn) % 4 == 0:
            thread = threading.Thread(target=insert_into_table, args=(conn, ))
            connection_pool.thread_list.append(thread)
        else:
            thread = threading.Thread(target=send_select, args=(conn, random.choice(requests)))
            connection_pool.thread_list.append(thread)

    try:
        connections_to_put = random.sample(connection_pool.used, random.randint(1, len(connection_pool.used)))
        for connection in connections_to_put:
            thread = threading.Thread(target=connection_pool.put_connection, args=(connection,))
            connection_pool.thread_list.append(thread)
    except ValueError:
        pass
    
    for thread in connection_pool.thread_list:
        try:
            thread.start()
        except RuntimeError:
            pass
    for thread in connection_pool.thread_list:
        try:
            thread.join()
        except RuntimeError:
            pass
    