

import collections


def _list_from_dict(items):
    for item in sorted(items):
        print "[%s]" % item, ", ".join(items[item])


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
        _list_from_dict(users)

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

    def permit_locking(self, user, database):
        # mysqldump needs to lock MyISAM tables
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("GRANT LOCK TABLES ON %s.* TO %%s@%%s;"
                                % database, (user, host))
        cursor.execute("FLUSH PRIVILEGES;")

    def permit_filing(self, user):
        # allows SELECT ... INTO OUTFILE '/path/to/file'
        hosts = self.get_users().get(user, set())
        cursor = self._connection.cursor()
        for host in hosts:
            cursor.execute("GRANT FILE ON *.* TO %s@%s;", (user, host))
        cursor.execute("FLUSH PRIVILEGES;")

    def get_database_privileges(self, user, database):
        # This only selects per-database privileges.
        # I still need to implement something to query
        # information_schema.user_privileges, and possibly
        # table_privileges and column_priviliges.
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT grantee, table_schema, privilege_type
            FROM information_schema.schema_privileges
            WHERE table_schema = '%s';""" % database)
        privileges = collections.defaultdict(set)
        for row in cursor.fetchall():
            row_user_side, _at, host_side = row[0].partition('@')
            row_user, host = row_user_side.strip("'"), host_side.strip("'")
            if row_user == user:
                privileges[row[2]].add(host)
        return privileges

    def list_database_privileges(self, user, database):
        privileges = self.get_database_privileges(user, database)
        _list_from_dict(privileges)

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


