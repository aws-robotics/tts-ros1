#! /usr/bin/env python

from __future__ import print_function
import sqlite3
import json
import os.path
from os import mkdir
import rospy


class DB(object):
    """DB a class to manage the database for tracking cached files"""

    def __init__(self, db_location='/tmp/polly.db'):
        """Sets up and returns the database for tracking cached files

        The database has two tables. The first table has a row for every
        utterance that is currently stored. The second table just has a
        single row with a single item tracking the total size of files
        stored.

        :param location: The location where the database is stored
        :return: The database as a sqlite3 connection
        """
        db_location = os.path.expanduser(db_location)

        dir_name = os.path.dirname(db_location)
        if not os.path.exists(dir_name):
            mkdir(dir_name)

        try:
            self.conn = sqlite3.connect(db_location)
        except sqlite3.OperationalError as e:
            rospy.logerr(
                "unable to connect to database at location: %s",
                format(db_location))
            rospy.logerr("error: %s", format(e))
            raise
        self.conn.row_factory = sqlite3.Row
        self.make_db()

    def ex(self, command, *args):
        """ex execute the passed in command and save it to the
        database immediately.

        :command: the command to run
        :*args: arguments which need to be passed in

        Returns: the sqlite cursor object with available return
        elements from the db
        """
        # this with statement will auto commit
        with self.conn:
            if args:
                to_return = self.conn.execute(command, args)
            else:
                to_return = self.conn.execute(command)
        return to_return

    def __del__(self):
        self.conn.close()

    def make_db(self):
        self.ex('''CREATE TABLE IF NOT EXISTS cache (
            hash text PRIMARY KEY,
            file text NOT NULL,
            audio_type text NOT NULL,
            last_accessed integer NOT NULL,
            size integer NOT NULL
            );''')

        self.ex('''CREATE TABLE IF NOT EXISTS size (
            id integer PRIMARY KEY,
            total_size integer NOT NULL,
            num_files integer NOT NULL
            );''')

        self.ex('''insert or ignore into size (
            id, total_size, num_files)
            values (1,0,0);''')
