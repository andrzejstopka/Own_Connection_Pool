import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler


class PoolError(psycopg2.Error):
    pass

class ConnectionPool:
    
    def __init__(self, dsn):
        self.pool = []
        self.used = []
        self.max_connections = 100
        self.dsn = dsn   # "postgres://YourUserName:YourPassword@YourHostname:5432/YourDatabaseName"
        self.run_schedule()
        self.create_start_connections()
        

    def create_start_connections(self):
        for _ in range(10):
            self.create_new_connection(True)

    def create_new_connection(self, initialization: bool):
        try:
            connection = psycopg2.connect(self.dsn)
            self.pool.append(connection)
            if initialization == False:  # when 11th connection is comming, I made it this way to avoid repeating
                self.used.append(connection)
            return connection
        except PoolError:
            self.create_new_connection(self, initialization)

    def get_connection(self):
        if len (self.pool) >= self.max_connections:
            raise PoolError("Connection Pool is full, you must wait for connection")
        for connection in self.pool:
            if connection not in self.used:
                self.used.append(connection)
                return connection
        return self.create_new_connection(False)

    def put_connection(self, connection):
        connection.close()
        self.used.remove(connection)

    def free_up_resources(self):
        print("Checking the number of connections...")
        deleted_connections = 0
        for connection in self.pool:
            if len(self.pool) <= 10:
                break
            if connection not in self.used:
                self.pool.remove(connection)
                deleted_connections += 1
        print(f"{deleted_connections} connections have been deleted.")

    def run_schedule(self):
        self.sched = BackgroundScheduler()
        self.sched.add_job(self.free_up_resources, "interval", seconds=10)
        self.sched.start()
        



    



    
        
