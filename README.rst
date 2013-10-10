
Dubbabear
=========

Dubbabear is an attempt to make database administration
a more pleasant experience.  Judge for yourself::

    connection = mysql.MySQLConnection(mysql.DEFAULTS)
    connection.add_database('foobar')
    connection.list_databases()
    allow_access_from = ['localhost', '127.0.0.1']
    connection.add_user('eduardo', allow_access_from, 'P@ssw0rd')
    connection.permit_reading('eduardo', 'foobar')

Unlike the many ORMs in existence this is focused on database
administration.

Currently only supporting MySQL.

