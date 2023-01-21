import psycopg2


class ConnectionPool:
    def __init__(self, dsn):
        self.pool = []
        self.used = []
        self.max_connections = 100
        self.dsn = dsn   # "postgres://YourUserName:YourPassword@YourHostname:5432/YourDatabaseName";

        self.create_start_connections(True)

    def create_start_connections(self):
        for _ in range(10):
            self.create_new_connection()

    def create_new_connection(self, initialization: bool):
        connection = psycopg2.connect(self.dsn)
        self.pool.append(connection)
        if initialization == False:  # when 11th connection is comming, i made it this way to avoid repeating
            self.used.append(connection)
        return connection

    def get_connection(self):
        for connection in self.pool:
            if connection not in self.used:
                self.used.append(connection)
                return connection
        return self.create_new_connection(False)


    

        
