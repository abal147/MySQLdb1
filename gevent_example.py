#!/usr/bin/python

import MySQLdb
import gevent
import sys

from MySQLdb.constants import ASYNC
from MySQLdb import cursors

def wait_for_action(connection):
    operation = connection.get_blocking_operation()
    fd = connection.get_file_descriptor()
    if operation == ASYNC.NET_NONBLOCKING_READ:
        gevent.socket.wait_read(fd)
    elif operation == ASYNC.NET_NONBLOCKING_WRITE or operation == ASYNC.NET_NONBLOCKING_CONNECT:
        gevent.socket.wait_write(fd)
    else:
        raise RuntimeError("Unexpected operation blocking type: %d" % operation)

def pump(connection, func, *params):
    while True:
        status = func(*params)

        if status == ASYNC.NET_ASYNC_COMPLETE:
            return

        wait_for_action(connection)

def connect_and_query(i):
    conn = MySQLdb.connect(nonblocking=True, user="test", host="127.0.0.1", passwd="", db="test")
    pump(conn, conn.nonblocking_connect_run)
    cursor = conn.cursor(cursorclass=cursors.NBCursor)
    cursor.execute("SELECT SLEEP(1)")
    pump(conn, cursor.execute_nonblocking)

    rows_seen = 0
    while True:
        status, row = cursor.fetchone_nonblocking()
        if status == ASYNC.NET_ASYNC_NOT_READY:
            wait_for_action(conn)
        if row is None: break
        rows_seen += 1

    conn.close()
    return rows_seen

def main(args):
    events = []
    for i in range(1000):
        events.append(gevent.spawn(connect_and_query, i))

    gevent.joinall(events)
    total_rows = 0
    for job in events:
        total_rows += job.value
    print "%d total rows" % total_rows

main(sys.argv[1:])
