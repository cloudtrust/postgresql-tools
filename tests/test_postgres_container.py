#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import re
import pytest
import logging
#import psycopg2
import time
import datetime
import calendar

import dateutil.parser

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

        while (tic_tac < max_timeout) and (psql_is_up == False):
            # check if monit started postgresql
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                postgresql_status = check_service().exit_code
                if (postgresql_status == 0):
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

        while (tic_tac < max_timeout) and (psql_is_up == False):
            # check if monit started postgresql
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                postgresql_status = check_service().exit_code
                if (postgresql_status == 0):
                    psql_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert psql_is_up == True

    def test_no_error_monit_log(self, settings):
        """
        Test to check that when running the container systemd starts influxdb and there is no error in the monit logs.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        # message in syslog when there are no errors
        no_error_status = "No entries"

        # stop and restart the container
        stop_docker = docker.bake("stop", container_name)
        logger.debug(stop_docker)
        stop_docker()

        restart_docker = docker.bake("start", container_name)
        logger.debug(restart_docker)
        restart_docker()
        time.sleep(2)

        # docker inspect --format='{{.State.Status}} container
        check_status = docker.bake("inspect", "--format='{{.State.StartedAt}}'", container_name)
        logger.debug(check_status)
        last_started_date = dateutil.parser.parse(check_status().stdout.rstrip()).replace(tzinfo=None)

        # check in journalctl if there are any errors since the container last started
        get_monit_log = docker.bake("exec", container_name, "journalctl", "-u", "monit", "--since", last_started_date,
                                    "-p", "err", "-b")
        logger.debug(get_monit_log)
        monit_log = get_monit_log().stdout.decode("utf-8")
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
                if (monit_status == 0):
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

        status = re.search(restart_status, monit_restart)
        assert status is not None
