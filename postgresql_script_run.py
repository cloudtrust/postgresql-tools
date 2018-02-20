#!/usr/bin/env python
# -*- coding: utf-8 -*-

author = "Sonia Bogos"
maintainer = "Sonia Bogos"
version = "0.0.1"

from postgresql_lib import script
import sys
import json
import logging
import argparse
from z3c.schema import ip
import psycopg2
import z3c.schema

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.postgresql_script_run")
logger.setLevel(logging.DEBUG)

prog_name = sys.argv[0]
parser = argparse.ArgumentParser(prog="{pn} {v}".format(pn=prog_name, v=version),
                                 description="module calls script-py to execute queries in postgresql")
usage = """{pn} [options]
    """.format(
    pn=prog_name
)

parser.add_argument(
    '--scripts',
    dest="scripts",
    help='Paths of the sql script: Ex : ../keycloack.sql',
    nargs='+',
    required=True
)

parser.add_argument(
    '--config-file',
    dest="path",
    help='Path of the database config: Ex : ../postgresql.json',
    type=str,
    required=False
)

parser.add_argument(
    '--ip',
    dest="ip",
    help='IP of the database config: Ex : "127.0.0.1"',
    type=str,
    required=True
)

parser.add_argument(
    '--user',
    dest="user",
    help='Username to connect to the database: Ex : "postgres"',
    type=str,
    required=False
)

parser.add_argument(
    '--password',
    dest="password",
    help='Password of the user that connects to the database: Ex : "1234"',
    type=str,
    required=False
)



def main():
    """

    :return:
    """
    args = parser.parse_args()
    sql_files = args.scripts
    config_file = args.path
    ip_db = args.ip
    user = args.user
    password = args.password

    # validate ip
    try:
        valid_ip = ip.IPAddress()
        valid_ip.validate(ip_db)
    except z3c.schema.ip.interfaces.NotValidIPAdress as e:
        logger.debug(e)
        raise e

    if config_file:
        # load the psql config file
        logger.info("loading config file from {}".format(config_file))
        config = {}
        try:
            with open(config_file) as json_data:
                config = json.load(json_data)
                user = config['user']
                password = config['password']

        except IOError as e:
            logger.debug(e)
            raise IOError("Config file {path} not found".format(path=config_file))

    else:
        if not user or not password:
            raise Exception("Incomplete credentials given as arguments")

    con = None

    try:
        logger.info("connecting to postgres with user {}".format(user))
        con = psycopg2.connect(host=ip_db, user=user, password=password)
    except Exception as e:
        logger.debug(e)
        if con:
            con.rollback()
        sys.exit(1)

    if len(sql_files) % 2 != 0:
        raise Exception("We should have pairs of scripts: script + rollback_script")

    for i in range(0, len(sql_files),2):
        sql_file = sql_files[i]
        rollback_sql_file = sql_files[i+1]

        logger.info("loading sql file from {0}".format(sql_file))
        logger.info("loading rollback sql file from {0}".format(rollback_sql_file))

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


if __name__ == "__main__":
    main()