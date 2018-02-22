#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import script
import pytest
import logging
import psycopg2
import sys

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.postgresql_lib.test_script")
logger.setLevel(logging.DEBUG)

@pytest.mark.usefixtures('psql_settings', scope='class')
class TestScript():
    """Class to test the python script.py."""

    def test_script_working(self, psql_settings):
        """Test to check that the script does the changes we expect him to do."""

        script_create_user = "CREATE USER test_script;"
        user = "test_script"
        script_drop_user = "DROP USER test_script;"
        config = psql_settings


        con = None
        try:
            logger.info("connecting to postgres with user {}".format(config['user']))
            con = psycopg2.connect(host=config['ip'], user=config['user'],
                                   password=config['password'])
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            print("Error %s" % e)
            sys.exit(1)

        script.PostgresqlScriptExecutor().run(con, script_create_user, script_drop_user)

        try:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{0}'".format(user))
            assert cur.rowcount == 1
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error %s" % e)
            sys.exit(1)

        script.PostgresqlScriptExecutor().run(con, script_drop_user, script_create_user)


        try:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{0}'".format(user))
            assert cur.rowcount == 0
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error %s" % e)
            sys.exit(1)

        if con:
            con.close()
            logger.info("closed connection to postgresql")

    def test_rollback(self, psql_settings):
        """Test to check that the rollback works: if an error occurs, we undo all the modifications."""

        script_create_user = "CREATE USER test_script;\n CREATE invalid_syntax;"
        user = "test_script"
        script_drop_user = "DROP USER test_script;"
        config = psql_settings

        con = None
        try:
            logger.info("connecting to postgres with user {}".format(config['user']))
            con = psycopg2.connect(host=config['ip'], user=config['user'],
                                   password=config['password'])
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            print("Error %s" % e)
            sys.exit(1)


        print(script.PostgresqlScriptExecutor().run(con, script_create_user, script_drop_user))

        try:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{0}'".format(user))
            assert cur.rowcount == 0
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error %s" % e)
            sys.exit(1)

        if con:
            con.close()
            logger.info("closed connection to postgresql")

