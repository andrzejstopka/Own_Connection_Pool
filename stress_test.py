import time
import random
from connection_pool import ConnectionPool, PoolError
import time



connection_pool = ConnectionPool("postgres://postgres:superuser@localhost:5432/connection_pool")
elapsed_time = 0
start_time = time.time()


while elapsed_time <= 600:  # 600 seconds == 10 minutes
    for _ in range(random.randint(1, 40)):
        connection_pool.get_connection()
    time.sleep(random.randint(1, 5))
    connections_to_put = random.sample(connection_pool.used, random.randint(1, 15))
    for connection in connections_to_put:
        connection_pool.put_connection(connection)
    time.sleep(random.randint(1, 5))
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    



    




