#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#     Chervine Majeri Kasmaei, chervine.majeri@elca.ch

import sys
import json
import logging
import argparse
import psycopg2
import jsonschema
import getpass

import z3c.schema

from postgresql_lib import script as pgscript
from z3c.schema import ip

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

version="1.0"
prog_name = sys.argv[0]
parser = argparse.ArgumentParser(prog="{pn} {v}".format(pn=prog_name, v=version))
usage = """{pn} [options]

Execute scripts and dedicated rollback scripts on postgresql

    Scripts can possess an equivalent rollback script based on the file name
    script.sql          : script to execute
    script.sql.rollback : the rollback script for script.sql

""".format(
    pn=prog_name
)

parser.add_argument(
    '--sql-script',
    dest="script",
    help='Paths of the sql script: Ex : ../keycloack.sql',
    required=False
)

parser.add_argument(
    '--sql-script-rollback',
    dest="rollback_script",
    help='Paths of the rollback sql script: Ex : ../keycloack_rollback.sql',
    required=False
)

parser.add_argument(
    '--config',
    dest="config",
    help='Path to the config file: Ex : ../postgresql.json',
    type=str,
    required=False,
)

parser.add_argument(
    '--host',
    dest="host",
    help='IP/Hostname running postgresql. Ex : "127.0.0.1"',
    type=str,
    required=False,
)

parser.add_argument(
    '--port',
    dest="port",
    help='Connection port, defaults to 5432',
    type=int,
    required=False,
)

parser.add_argument(
    '--username',
    dest="user",
    help='Username to connect to the database: Ex : "postgres"',
    type=str,
    required=False,
)

parser.add_argument(
    '--password',
    dest="password",
    help='Password of the user that connects to the database: Ex : "1234"',
    type=str,
    required=False,
)

parser.add_argument(
    '--debug',
    dest="debug",
    default=False,
    action="store_true",
    help='Enable debug'
)


def validate_json(json_file, json_schema):
    #Validate the incoming json file
    try:
        jsonschema.validate(
            json_file,
            json_schema
        )
    except jsonschema.ValidationError as e:
        logger.debug("Error : {m}".format(m=e))
        raise jsonschema.ValidationError
    except jsonschema.SchemaError as e:
        logger.debug("Error : {m}".format(m=e))
        raise jsonschema.SchemaError


if __name__ == "__main__":
    args = parser.parse_args()
    debug = args.debug
    logger = logging.getLogger("postgres_tools.postgresql_execute_script")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    json_schema = {
        "$schema": "http://json-schema.org/schema#",
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "user": {"type": "string"},
            "password": {"type": "string"},
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "script": {"type": "string"},
            "rollback_script": {"type": "string"},
        }
    }

    ## Take commandline arguments. Those have highest precedence.
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    script = args.script
    rollback_script = args.rollback_script
    config_file = args.config

    ## Check against config parameters, if the variable isn't already defined
    if config_file:
        logger.info("loading config file from {path}".format(path=config_file))
        config = {}
        try:
            with open(config_file) as json_data:
                config = json.load(json_data)
                validate_json(config, json_schema)
                user = user or config.get('user')
                password = password or config.get('password')
                host = host or config.get('host')
                port = port or config.get('port')
                script = script or config.get('script')
                rollback_script = rollback_script or config.get('rollback_script')
        except IOError as e:
            logger.debug(e)
            raise IOError("Config file {path} not found".format(path=config_file))

    ## Use default arguments (where applicable). If not already defined.
    user = user or getpass.getuser()

    psql_exec = pgscript.PostgresqlScriptExecutor()

    logger.info("loading sql file from {file}".format(file=script))
    logger.info("loading rollback sql file from {file}".format(file=rollback_script))

    try:
        with open(script, "r") as f:
            commands = f.read()
        f.close()
    except Exception as e:
        logger.debug(e)
        raise Exception("Sql file {path} cannot be read".format(path=script))

    rollback_commands=""
    if rollback_script:
        try:
            with open(rollback_script, "r") as f:
                rollback_commands = f.read()
            f.close()
        except Exception as e:
            logger.debug(e)
            raise Exception("Rollback sql file {path} cannot be read".format(path=rollback_script))

    con = None
    try:
        logger.info("connecting to postgres with user {name}".format(name=user))
        con = psycopg2.connect(host=host, user=user, password=password, port=port)
    except Exception as e:
        logger.debug(e)
        sys.exit(1)

    try:
        res = psql_exec.run(con, commands, rollback_commands)
        logger.debug(
            json.dumps(
                res,
                sort_keys=True,
                indent=4,
                separators=(',', ': ')
            )
        )

    except Exception as e:
        logger.debug(e)
        logger.info("Unexpected failure when running script. Closing connection.")
        con.close()
        sys.exit(1)

    con.close()
    logger.info("closed connection to postgresql")
