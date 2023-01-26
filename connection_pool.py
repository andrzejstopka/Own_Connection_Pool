import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
import threading



class PoolError(psycopg2.Error):
    pass

class ConnectionPool:
    
    def __init__(self, dsn):
        self.pool = []
        self.used = []
        self.max_connections = 100
        self.dsn = dsn   # "postgres://YourUserName:YourPassword@YourHostname:5432/YourDatabaseName"
        self.pool_lock = threading.Lock()
        self.used_lock = threading.Lock()
        self.thread_list = []
        self.run_schedule()
        self.create_start_connections()

        

    def create_start_connections(self):
        for _ in range(10):
            self.create_new_connection(True)

    def create_new_connection(self, initialization: bool):
        try:
            connection = psycopg2.connect(self.dsn)
            self.pool_lock.acquire()
            self.pool.append(connection)
            self.pool_lock.release()
            if initialization == False:  # when 11th connection is comming, I made it this way to avoid repeating
                self.used_lock.acquire()
                self.used.append(connection)
                self.used_lock.release()
            print("Connection successfully established")
            return connection
        except PoolError:
            self.create_new_connection(self, initialization)

    def get_connection(self):
        try:
            if len (self.pool) >= self.max_connections:
                raise PoolError
            for connection in self.pool:
                if connection not in self.used:
                    self.used_lock.acquire()
                    self.used.append(connection)
                    self.used_lock.release()
                    print("Connection successfully established")
                    return connection
            self.create_new_connection(False)
        except PoolError:
            print("Connection Pool is full, you must wait for connection")

    def put_connection(self, connection):
        self.used_lock.acquire()
        print("The connection was successfully terminated")
        self.used.remove(connection)
        self.used_lock.release()

    def free_up_resources(self):
        print("-------------------------------------------------------")
        print("Checking the number of connections...")
        print(f"Pool length: {len(self.pool)} - Used length: {len(self.used)}")
        deleted_connections = 0
        self.pool_lock.acquire()
        self.used_lock.acquire()
        for connection in self.pool:
            if len(self.pool) <= 10:
                break
            if connection not in self.used:
                self.pool.remove(connection)
                deleted_connections += 1   
        self.pool_lock.release()
        self.used_lock.release() 
        print(f"{deleted_connections} connections have been deleted.")
        print(f"Pool length: {len(self.pool)} - Used length: {len(self.used)}")
        print("-------------------------------------------------------")
        
        


    def run_schedule(self):
        self.sched = BackgroundScheduler()
        self.sched.add_job(self.free_up_resources, "interval", seconds=60)
        self.sched.start()

