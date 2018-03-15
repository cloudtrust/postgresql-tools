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
class TestContainerPostgresql():
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
        con = None
        try:
            logger.info("connecting to postgres with user {user}".format(user=psql_settings['user']))
            con = psycopg2.connect(host=psql_settings['host_ip'], user=psql_settings['user'],
                               password=psql_settings['password'])
            # con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = con.cursor()

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
            sys.exit(1)
        finally:
            if con:
                con.close()
                logger.info("closed connection to postgresql")

    def test_data_consistency(self, settings, psql_settings):
        """
        Test to check that the modifications done in Postgresql are present after the container was stopped.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']

        con = None
        try:
            logger.info("connecting to postgres with user {user}".format(user=psql_settings['user']))
            con = psycopg2.connect(host=psql_settings['host_ip'], user=psql_settings['user'],
                                   password=psql_settings['password'])
            cur = con.cursor()

            # create an user
            username = "test_postgresql"
            cur.execute("CREATE USER {user};".format(user=username))
            logger.debug("CREATE USER {user}".format(user=username))
            con.commit()
        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error {error}".format(error=e))
            sys.exit(1)
        finally:
            if con:
                con.close()
                logger.info("closed connection to postgresql")

        stop_container = docker.bake("stop", container_name)
        logger.debug(stop_container)
        stop_container()

        restart_container = docker.bake("restart", container_name)
        logger.debug(restart_container)
        restart_container()


        psql_is_up = False
        while (psql_is_up == False):
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.debug(check_service)

            try:
                postgresql_status = check_service().exit_code
                if (postgresql_status == 0):
                    psql_is_up = True
                    logger.info("{service} is running".format(service=service_name))
            except Exception as e:
                logger.info("{service} is not yet running".format(service=service_name))

        con = None
        try:
            logger.info("connecting again to postgres with user {user}".format(user=psql_settings['user']))
            con = psycopg2.connect(host=psql_settings['host_ip'], user=psql_settings['user'],
                                   password=psql_settings['password'])
            cur = con.cursor()

            # check if the user created exists

            logger.debug("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=username))
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=username))
            assert cur.rowcount == 1

            # remove the created user
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
            sys.exit(1)

        finally:
            if con:
                con.close()
                logger.info("closed connection to postgresql")