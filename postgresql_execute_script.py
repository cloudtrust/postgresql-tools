#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import sys
import json
import logging
import argparse
import psycopg2
import jsonschema

import z3c.schema

from postgresql_lib import script
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

    SQL scripts are stored within /scripts
    Scripts can possess an equivalent rollback script based on the file name
    script.sql          : script to execute
    script.sql.rollback : the rollback script for script.sql

""".format(
    pn=prog_name
)

parser.add_argument(
    '--sql-scripts',
    dest="scripts",
    help='Paths of the sql script: Ex : ../keycloack.sql',
    nargs='+',
    required=True
)

parser.add_argument(
    '--database-config-file',
    dest="path",
    help='Path of the database config: Ex : ../postgresql.json',
    type=str,
    required=False
)

parser.add_argument(
    '--database-server-ip',
    dest="db_server_ip",
    help='IP of the database config: Ex : "127.0.0.1"',
    type=str,
    required=True
)


parser.add_argument(
    '--database-user',
    dest="db_user",
    help='Username to connect to the database: Ex : "postgres"',
    type=str,
    required=False
)

parser.add_argument(
    '--database-password',
    dest="db_password",
    help='Password of the user that connects to the database: Ex : "1234"',
    type=str,
    required=False
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
    """

    :return:
    """
    # parse args
    ##
    args = parser.parse_args()

    # debug
    ##
    debug = args.debug

    # set debug level
    ##
    logger = logging.getLogger("postgres_tools.postgresql_execute_script")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.CRITICAL)

    # assign and validate
    ##

    # Database server IP
    try:
        db_server_ip = args.db_server_ip
        ip_addr_validator = z3c.schema.ip.IPAddress()
        ip_addr_validator.validate(db_server_ip)
    except z3c.schema.ip.interfaces.NotValidIPAdress as e:
        logger.debug(e)
        raise z3c.schema.ip.interfaces.NotValidIPAdress(
            "db_server_ip : {ip} is not a valid ip".format(ip=db_server_ip)
        )

    # Database config parameters
    ##
    config_file = args.path
    db_user = args.db_user
    db_password = args.db_password

    db_json_schema = {
        "$schema": "http://json-schema.org/schema#",
        "required": ["user", "password"],
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "user": {"user": "string"},
            "password": {"password": "string"}
        }
    }

    if not db_user or not db_password:
        if config_file:
            # load the psql config file
            logger.info("loading config file from {path}".format(path=config_file))
            config = {}
            try:
                with open(config_file) as json_data:
                    config = json.load(json_data)
                    validate_json(config, db_json_schema)
                    db_user = config['user']
                    db_password = config['password']

            except IOError as e:
                logger.debug(e)
                raise IOError("Config file {path} not found".format(path=config_file))

        else:
            raise Exception("Incomplete credentials given as arguments")

    # Sql scripts
    ##
    sql_files = args.scripts

    con = None

    try:
        logger.info("connecting to postgres with user {user}".format(user=db_user))
        con = psycopg2.connect(host=db_server_ip, user=db_user, password=db_password)
    except Exception as e:
        logger.debug(e)
        if con:
            con.rollback()
        sys.exit(1)

    if len(sql_files) % 2 != 0:
        raise Exception("We should have pairs of scripts: script + rollback_script")

    for i in range(0, len(sql_files), 2):
        sql_file = sql_files[i]
        rollback_sql_file = sql_files[i+1]

        logger.info("loading sql file from {file}".format(file=sql_file))
        logger.info("loading rollback sql file from {file}".format(file=rollback_sql_file))

        try:
            with open(sql_file, "r") as f:
                commands = f.read()
            f.close()

        except Exception as e:
            logger.debug(e)
            raise Exception("Sql file {path} cannot be read".format(path=sql_file))

        try:
            with open(rollback_sql_file, "r") as f:
                rollback_commands = f.read()
            f.close()

        except Exception as e:
            logger.debug(e)
            raise Exception("Rollback sql file {path} cannot be read".format(path=rollback_sql_file))

        res = script.PostgresqlScriptExecutor().run(con, commands, rollback_commands)
        logger.debug(res)

    # close the postgresql connection
    if con:
        con.close()
        logger.info("closed connection to postgresql")


