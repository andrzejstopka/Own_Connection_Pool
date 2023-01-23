import psycopg2
import sched, time

class PoolError(psycopg2.Error):
    pass

class ConnectionPool:
    def __init__(self, dsn):
        self.pool = []
        self.used = []
        self.max_connections = 100
        self.dsn = dsn   # "postgres://YourUserName:YourPassword@YourHostname:5432/YourDatabaseName";

        self.create_start_connections(True)
        self.free_up = sched.scheduler(time.time, time.sleep)
        self.free_up.enter(60, 1, self.free_up_resources, (self.free_up, ))
        self.free_up.run() 

    def create_start_connections(self):
        for _ in range(10):
            self.create_new_connection()

    def create_new_connection(self, initialization: bool):
        connection = psycopg2.connect(self.dsn)
        self.pool.append(connection)
        if initialization == False:  # when 11th connection is comming, I made it this way to avoid repeating
            self.used.append(connection)
        return connection

    def get_connection(self):
        if len (self.pool) >= self.max_connections:
            raise PoolError("Connection Pool is full, you must wait for connection")
        for connection in self.pool:
            if connection not in self.used:
                self.used.append(connection)
                return connection
        return self.create_new_connection(False)

    def free_up_resources(self, runnable_task):
        runnable_task.enter(60, 1, self.free_up_resources, (runnable_task,))
        print("Checking the number of connections...")
        deleted_connections = 0
        for connection in self.pool:
            if len(self.pool) <= 10:
                break
            if connection not in self.used:
                self.pool.remove(connection)
                deleted_connections += 1
        return f"{deleted_connections} connections have been deleted."
                





    

        
