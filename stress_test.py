import time
import random
from connection_pool import ConnectionPool, PoolError
import time
import threading



connection_pool = ConnectionPool("postgres://postgres:superuser@localhost:5432/connection_pool")
elapsed_time = 0
start_time = time.time()
  
while elapsed_time <= 600:    # 600 seconds == 10 minutes
    for _ in range(random.randint(1, 40)):
        thread = threading.Thread(target=connection_pool.get_connection)
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
