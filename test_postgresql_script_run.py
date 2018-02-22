#!/usr/bin/env python
# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import pytest
import logging
import sh

from sh import python3

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.test_postgresql_script_run")
logger.setLevel(logging.DEBUG)


class TestcriptPSQLScriptRun():
    """Class to test the python script postgresql_script_run."""

    def test_missing_arguments(self):
        """Test to check that missing arguments raise an exception."""

        script_name = "postgresql_script_run.py"

        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--scripts", "keycloak.sql")
            exec_script()
        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--ip", "127.0.0.1")
            exec_script()
        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--ip", "127.0.0.1", "--scripts")
            exec_script()


    def test_insufficient_credentials(self):
        """Test to check that insufficient credentials (username, password) raise an exception."""

        script_name = "postgresql_script_run.py"

        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--scripts", "keycloak.sql", "--ip", "127.0.0.1", "--user", "test")
            exec_script()
        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--scripts", "keycloak.sql", "--ip", "127.0.0.1", "--password", "test")
            exec_script()
        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--scripts", "keycloak.sql", "--ip", "127.0.0.1", "--user", "test", "--password")
            exec_script()

    def test_inexistent_sql_file(self):
        """Test to check that an inexistent file, given as argument, raises an exception."""

        script_name = "postgresql_script_run.py"

        with pytest.raises(Exception):
            exec_script = python3.bake(script_name, "--scripts", "not_existent_file.sql", "--ip", "127.0.0.1", "--config-file", "psql.json")
            exec_script()

    def test_invalid_ip(self):
        """Test to check that an invalid ip, given as argument, raises an exception."""

        script_name = "postgresql_script_run.py"

        with pytest.raises(sh.ErrorReturnCode_1):
            exec_script = python3.bake(script_name, "--scripts", "keycloak.sql", "keycloak_rollback.sql", "--ip", "127.0.0",
                                   "--config-file", "psql.json")
            print("trying to get stderr")
            print("this " + exec_script().stderr)
            val = exec_script()
            print(val.exit_code)

    def test_script_working(self):
        """Test to check that the script is doing what he is expected to do."""

        script_name = "postgresql_script_run.py"
        create_user = python3.bake(script_name, "--scripts", "test_create_user.sql", "test_drop_user.sql", "--ip", "127.0.0.1",
                                   "--config-file", "./test_config/psql.json")
        create_user()

        drop_user = python3.bake(script_name, "--scripts", "test_drop_user.sql", "test_create_user.sql", "--ip", "127.0.0.1",
                                   "--config-file", "./test_config/psql.json")
        drop_user()



