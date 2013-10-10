

import collections


class MySQLConnection:

    def __init__(self, configuration):
        import MySQLdb
        self._connection = MySQLdb.connect(
            host = configuration['host'],
            port = configuration['port'],
            user = configuration['user'],
            passwd = configuration['password'],
            db = configuration['database'],
        )

    def list_databases(self):
        cursor = self._connection.cursor()
        cursor.execute("SHOW DATABASES;")
        for row in cursor.fetchall():
            print row[0]

    def add_database(self, name):
        cursor = self._connection.cursor()
        cursor.execute("CREATE DATABASE %s;" % name)

    def remove_database(self, name):
        cursor = self._connection.cursor()
        cursor.execute("DROP DATABASE %s;" % name)

    def get_users(self):
        cursor = self._connection.cursor()
        cursor.execute("SELECT User, Host FROM mysql.user;")
        users = collections.defaultdict(set)
        for user, host in cursor.fetchall():
            users[user].add(host)
        return users

    def list_users(self):
        users = self.get_users()
        for user in sorted(users):
            print "[%s]" % user, ", ".join(users[user])

    def add_user(self, user, hosts, password):
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("CREATE USER %s@%s IDENTIFIED BY %s;",
                                (user, host, password))

    def remove_user(self, user):
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("DROP USER %s@%s;", (user, host))

    def change_password(self, user, password):
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("SET PASSWORD FOR %s@%s = PASSWORD(%s);",
                                (user, host, password))

    def permit_administration(self, user, database):
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("GRANT ALL ON %s.* TO %%s@%%s;" % database,
                                (user, host))
        cursor.execute("FLUSH PRIVILEGES;")

    def permit_writing(self, user, database):
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("GRANT INSERT, SELECT, UPDATE, DELETE, SHOW VIEW ON %s.* TO %%s@%%s;"
                                % database, (user, host))
        cursor.execute("FLUSH PRIVILEGES;")

    def permit_reading(self, user, database):
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("GRANT SELECT, SHOW VIEW ON %s.* TO %%s@%%s;"
                                % database, (user, host))
        cursor.execute("FLUSH PRIVILEGES;")

    def list_tables(self, database):
        cursor = self._connection.cursor()
        cursor.execute("SHOW TABLES IN %s;" % database)
        for row in cursor.fetchall():
            print row[0]

    def add_kv_table(self, database, table):
        # A two-column table only useful for testing.
        cursor = self._connection.cursor()
        cursor.execute("""
            CREATE TABLE %s.%s (
                mykey TEXT,
                myvalue TEXT
            );""" % (database, table))

    def remove_table(self, database, name):
        cursor = self._connection.cursor()
        cursor.execute("DROP TABLE %s.%s;" % (database, name))


DEFAULTS = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': '',
}


def test():
    connection = MySQLConnection(DEFAULTS)
    connection.list_databases()
    connection.list_users()


if __name__ == '__main__':
    test()


