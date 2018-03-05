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
    '--sql-script',
    dest="script",
    help='Paths of the sql script: Ex : ../keycloack.sql',
    required=True
)

parser.add_argument(
    '--sql-script-rollback',
    dest="script_rollback",
    help='Paths of the rollback sql script: Ex : ../keycloack_rollback.sql',
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
    '--debug',
    dest="debug",
    default=False,
    action="store_true",
    help='Enable debug'
)


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
        logger.setLevel(logging.INFO)

    # Postgresql username
    db_user = args.db_user
    if not db_user:
        raise Exception("Postgresql username missing")

    # PostgresqlScriptExecutor instance
    ##
    psql_exec = script.PostgresqlScriptExecutor()

    # Sql scripts
    ##
    sql_file = args.script
    sql_file_rollback = args.script_rollback

    logger.info("loading sql file from {file}".format(file=sql_file))
    logger.info("loading rollback sql file from {file}".format(file=sql_file_rollback))

    try:
        with open(sql_file, "r") as f:
            commands = f.read()
        f.close()

    except Exception as e:
        logger.debug(e)
        raise Exception("Sql file {path} cannot be read".format(path=sql_file))

    try:
        with open(sql_file_rollback, "r") as f:
            rollback_commands = f.read()
        f.close()

    except Exception as e:
        logger.debug(e)
        raise Exception("Rollback sql file {path} cannot be read".format(path=sql_file_rollback))

    con = None
    try:
        logger.info("connecting to postgres with user {name}".format(name=db_user))
        con = psycopg2.connect(user=db_user)
    except Exception as e:
        logger.debug(e)
        if con:
            con.rollback()
            con.close()
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
        if con:
            con.rollback()
        sys.exit(1)
    finally:
        # close the postgresql connection
        if con:
            con.close()
            logger.info("closed connection to postgresql")


