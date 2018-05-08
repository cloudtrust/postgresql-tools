#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#

import re
import pytest
import logging
import time
import psycopg2

import dateutil.parser

from sh import docker

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.tests.test_postgres_container")
logger.setLevel(logging.DEBUG)


@pytest.mark.usefixtures('psql_settings', 'settings', scope='class')
class TestContainerPostgresql():
    """
        Class to test the posgresql container.
    """

    def test_systemd_running_postgresql(self, settings):
        """
        Test to check if systemd is running postgresql.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']

        command_postgresql = (
        "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/postgresql_2eservice",
        "org.freedesktop.systemd1.Unit", "ActiveState")
        active_status = '"active"'

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, command_postgresql)
        logger.debug(check_service)

        # check the return value
        postgresql_status = check_service().stdout.decode("utf-8")
        logger.debug(postgresql_status)

        status = re.search(active_status, postgresql_status)
        assert status is not None

    def test_systemd_running_monit(self, settings):
        """
        Test to check if systemd is running monit.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']

        command_monit = (
        "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/monit_2eservice",
        "org.freedesktop.systemd1.Unit", "ActiveState")
        active_status = '"active"'

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, command_monit)
        logger.debug(check_service)

        # check the return value
        monit_status = check_service().stdout.decode("utf-8")
        logger.debug(monit_status)

        status = re.search(active_status, monit_status)
        assert status is not None

    def test_container_running(self, settings):
        """
        Test to check if the container is running.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        running_status = 'running'
        container_name = settings['container_name']

        # docker inspect --format='{{.State.Status}} container
        check_status = docker.bake("inspect", "--format='{{.State.Status}}'", container_name)
        logger.debug(check_status)

        status = re.search(running_status, check_status().stdout.decode("utf-8"))
        assert status is not None

    def test_monit_restarts_stopped_postgresl(self, settings):
        """
        Test to check if monit restarts a stopped postgresql.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']
        max_timeout = settings['psql_timeout']

        # stop postgresql
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "stop", service_name)
        logger.debug(stop_service)

        stop_service()

        tic_tac = 0
        psql_is_up = False

        while (tic_tac < max_timeout) and (not psql_is_up):
            # check if monit started postgresql
            time.sleep(1)

            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                postgresql_status = check_service().exit_code
                if postgresql_status == 0:
                    psql_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert psql_is_up == True

    def test_monit_restarts_killed_postgresl(self, settings):
        """
        Test to check if monit restarts a killed postgresql.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']
        max_timeout = settings['psql_timeout']

        # kill postgresql
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "kill", service_name)
        logger.debug(stop_service)

        stop_service()

        tic_tac = 0
        psql_is_up = False

        while (tic_tac < max_timeout) and (not psql_is_up):
            # check if monit started postgresql
            time.sleep(1)

            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                postgresql_status = check_service().exit_code
                if postgresql_status == 0:
                    psql_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert psql_is_up == True

    def test_no_error_monit_log(self, settings):
        """
        Test to check that, when running the container, systemd starts postgresql and there is no error in the monit logs.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        # message in syslog when there are no errors
        no_error_status = "No entries"

        # docker inspect --format='{{.State.Status}} container
        check_status = docker.bake("inspect", "--format='{{.State.StartedAt}}'", container_name)
        logger.debug(check_status)
        last_started_date = dateutil.parser.parse(check_status().stdout.rstrip()).replace(tzinfo=None)

        time.sleep(3)

        # check in journalctl if there are any errors since the container last started
        get_monit_log = docker.bake("exec", container_name, "journalctl", "-u", "monit", "--since", last_started_date,
                                    "-p", "err", "-b")
        logger.debug(get_monit_log)

        monit_log = get_monit_log().stdout.decode("utf-8")
        logger.debug(monit_log)

        assert re.search(no_error_status, monit_log) is not None

    def test_systemd_restarts_monit(self, settings):
        """
        Test to check that if monit is down then systemd will restart it.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = "monit"
        max_timeout = settings['monit_timeout']

        # kill monit
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "kill", service_name)
        logger.debug(stop_service)

        stop_service()

        tic_tac = 0
        monit_is_up = False

        while (tic_tac < max_timeout) and (monit_is_up == False):
            # check if systemd starts monit
            time.sleep(1)

            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                monit_status = check_service().exit_code
                if monit_status == 0:
                    monit_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1

        assert monit_is_up == True

    def test_container_exposed_ports(self, settings):
        """
        Test to check if the correct ports are exposed.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        ports = settings['ports']

        check_ports = docker.bake("inspect", "--format='{{.Config.ExposedPorts}}'", container_name)
        logger.debug(check_ports)

        exposed_ports = check_ports().stdout.decode("utf-8")
        logger.debug(exposed_ports)

        for port in ports:
            assert re.search(port, exposed_ports) is not None

    def test_monit_always_restarts(self, settings):
        """
        Test to check if monit is configured to always restart.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """
        container_name = settings['container_name']

        command_monit = (
            "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/monit_2eservice",
            "org.freedesktop.systemd1.Service", "Restart")
        restart_status = '"always"'

        # docker exec -it busctl get-property
        check_monit_restart = docker.bake("exec", "-i", container_name, command_monit)
        logger.debug(check_monit_restart)

        # check the return value
        monit_restart = check_monit_restart().stdout.decode("utf-8")
        logger.debug(monit_restart)

        status = re.search(restart_status, monit_restart)
        assert status is not None

    def test_data_consistency(self, settings, psql_settings):
        """
        Test to check that the modifications done in Postgresql are present after the container was stopped.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']

        try:
            logger.info("connecting to postgres with user {user}".format(user=psql_settings['user']))
            with psycopg2.connect(host=psql_settings['host'], user=psql_settings['user'],
                               password=psql_settings['password']) as con:
                with con.cursor() as cur:

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

        while not psql_is_up:
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.debug(check_service)

            try:
                postgresql_status = check_service().exit_code
                if postgresql_status == 0:
                    psql_is_up = True
                    logger.info("{service} is running".format(service=service_name))
            except Exception as e:
                pass

        try:
            logger.info("connecting again to postgres with user {user}".format(user=psql_settings['user']))
            with psycopg2.connect(host=psql_settings['host'], user=psql_settings['user'],
                                  password=psql_settings['password']) as con:
                with con.cursor() as cur:

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

        finally:
            if con:
                con.close()
                logger.info("closed connection to postgresql")