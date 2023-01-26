import unittest
import psycopg2.extensions

from connection_pool import ConnectionPool, PoolError

class TestConnectionPool(unittest.TestCase):
    def setUp(self):
        self.connection_pool = ConnectionPool("postgres://postgres:superuser@localhost:5432/connection_pool")
        self.lenght_pool = len(self.connection_pool.pool)

    def test_create_start_connections(self):
        self.assertEqual(self.lenght_pool, 10)

    def test_create_new_connection(self):
        result1 = self.connection_pool.create_new_connection(False)
        result2 = self.connection_pool.create_new_connection(True)

        self.assertIsInstance(result1, psycopg2.extensions.connection)
        self.assertEqual(len(self.connection_pool.pool), self.lenght_pool + 2)
        self.assertIn(result1, self.connection_pool.pool)
        self.assertIn(result1, self.connection_pool.used)
        self.assertIsInstance(result2, psycopg2.extensions.connection)
        self.assertIn(result2, self.connection_pool.pool)
        self.assertNotIn(result2, self.connection_pool.used)

    def test_get_connection(self):
        result1 = self.connection_pool.get_connection()

        self.assertIsInstance(result1, psycopg2.extensions.connection)


    def test_put_connection(self):
        connection = self.connection_pool.get_connection()

        self.assertIn(connection, self.connection_pool.used)

        self.connection_pool.put_connection(connection)

        self.assertNotIn(connection, self.connection_pool.used)        

    
        
if __name__ == "__main__":
    unittest.main()

