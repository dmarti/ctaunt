#!/usr/bin/env python

from random import random
import time

from cassandra.cluster import Cluster

# cluster = Cluster(['127.0.0.1'])
cluster = Cluster(['127.0.0.1', '192.168.122.89'])
session = cluster.connect('system')
session.execute('''
    CREATE KEYSPACE IF NOT EXISTS scratch WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
''')

session = cluster.connect('scratch')
# session.execute('DROP COLUMNFAMILY users')
try:
    session.execute('''
    CREATE COLUMNFAMILY USERS (
        key varchar PRIMARY KEY,
        email varchar);
    ''')
except:
    pass

key = 0
while(True):
    key += 1
    session.execute('''
    INSERT INTO users (key,email) VALUES ('%s', 'joe@example.com')
    ''' % key)
    session.execute('''
    SELECT * FROM users
    ''')
    time.sleep(1 + random())
    print(key)
