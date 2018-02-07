#!/usr/bin/python
# -*- coding: utf-8 -*-

author = "Sonia Bogos"
maintainer = "Sonia Bogos"
version = "0.0.1"

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import json
import logging
import argparse

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.config")
logger.setLevel(logging.DEBUG)


prog_name = sys.argv[0]
parser = argparse.ArgumentParser(prog="{pn} {v}".format(pn=prog_name, v=version), description="module that executes the changes in postgres from the json file" )
usage = """{pn} [options]
""".format(
    pn=prog_name
)

parser.add_argument(
    '--path',
    dest="path",
    help='Path of the json file: Ex : ../config.json',
    type=str,
    required=True
)

args = parser.parse_args()
config_file = args.path

# load the json file

logger.info("loading config file from " + config_file)

try:
    with open(config_file) as json_data:
        config = json.load(json_data)

except IOError as e:
    logger.debug(e)
    raise IOError("Config file {path} not found".format(path=config_file))


# connect to postgresql
con = None

try:
    logger.info("connecting to postgres with user {}".format(config['credentials']['user']))
    con = psycopg2.connect(host='localhost', user=config['credentials']['user'], password=config['credentials']['password'])
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # create all the users
    for user in config['create_user']:
        options = ""
        for option in user['option']:
            options = options + option['key'] + " '%s' " % (option['value'],)

        cur.execute("CREATE USER {}".format(user['name']) + " WITH " + options + " ;")
        logger.info("created user {} with options {}".format(user['name'], options))

    # create all databases
    for database in config['create_database']:
        cur.execute("CREATE DATABASE {} ;".format(database['name']))
        logger.info("created database {}".format(database['name']))

    # grant
    for item in config['grant']:
        cur.execute("GRANT ALL ON DATABASE {} TO {} ;".format(item['database'], item['role_name']))
        logger.info("granted all to user {} on database {}".format(item['role_name'], item['database']))

    # alter users
    for user in config['alter_user']:
        cur.execute("ALTER USER {} WITH {} ;".format(user['role_specification'], user['option']))
        logger.info("changed user {} with {}".format(user['role_specification'], user['option']))

    con.commit()
except psycopg2.DatabaseError, e:
    logger.debug(e)
    if con:
        con.rollback()
    print 'Error %s' % e
    sys.exit(1)
finally:
    if con:
        con.close()
        logger.info("closed connection to postgresql")
