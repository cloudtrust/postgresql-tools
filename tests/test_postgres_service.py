#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#


import pytest
import logging
import psycopg2
import time

from sh import docker

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.tests.test_postgres_container")
logger.setLevel(logging.INFO)


@pytest.mark.usefixtures('psql_settings', 'settings', scope='class')
class TestServicePostgresql():
    """
        Class to test the posgresql service.
    """
    def test_postgresl(self, psql_settings):
        """
        Test to check if postgresql is functional.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        # test if one can do modifications on postgres db

        try:
            logger.info("connecting to postgres with user {user}".format(user=psql_settings['user']))

            with psycopg2.connect(host=psql_settings['host'], user=psql_settings['user'],
                               password=psql_settings['password']) as con:
                with con.cursor() as cur:

                    # check if we can create a user
                    username = "test_postgresql"
                    cur.execute("CREATE USER {user};".format(user=username))
                    logger.debug("CREATE USER {user}".format(user=username))
                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=username))
                    assert cur.rowcount == 1

                    # check if we can remove the user we just created
                    cur.execute("DROP USER {user};".format(user=username))
                    logger.debug("DROP USER {user};".format(user=username))
                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=username))
                    assert cur.rowcount == 0

                    con.commit()
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error {error}".format(error=e))

        finally:
            if con:
                con.close()
                logger.info("closed connection to postgresql")

