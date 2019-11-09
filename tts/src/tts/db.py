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

    def get_size(self):
        """Return the sum size of the files in the database

        Note: the actual on disk size could be smaller if files have
        been deleted without notifying the database. This will self
        resolve with time."""
        return self.ex('SELECT SUM(size) FROM cache')

    def get_num_files(self):
        """Return the number of files cached in the database"""
        return self.ex('SELECT Count(*) FROM cache').fetchone()

    def remove_file(self, fn):
        """Remove a file from the database and delete the file

        This function removes the file from the disk before 
        removing it from the database. This is the safest
        order to operate in, preventing items falling out
        of the cache while still existing. 

        Args:
            fn: the filename of the file to remove
        """
        if os.path.exists(fn):
            os.remove(fn)
        self.ex('delete from cache where file=?', fn)

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
