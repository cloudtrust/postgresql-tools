#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#



import psycopg2
import sys
import logging
import collections

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.postgresql_lib.script")
logger.setLevel(logging.INFO)

class PostgresqlScriptExecutor(object):
    @staticmethod
    def run(con, script, rollback_script):
        """

        :param con: connection to postgresql
        :param script: list of queries to execute
        :param rollback_script: list of queries to execute for rollback
        :return: transcript of the executed queries
        """
        res = collections.OrderedDict()
        counter = 0
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with con:
                with con.cursor() as cur:
                    # execute the sql script query by query
                    list_commands = script.split(";\n")
                    for command in list_commands:
                        if command and not command.isspace():
                            counter += 1
                            res[counter] = {}
                            res[counter]["command"] = "{0}".format(command)
                            cur.execute(command)
                            logger.info(command)
                            res[counter]["status"] = "{0}".format(cur.statusmessage)
            con.commit()
        except Exception as e:
            logger.info(e)
            if con:
                # execute the rollback script
                with con.cursor() as cur:
                    list_commands = rollback_script.split(";\n")
                    for command in list_commands:
                        if command and not command.isspace():
                            counter += 1
                            res[counter] = {}
                            res[counter]["command"] = "{0}".format(command)
                            cur.execute(command)
                            logger.info(command)
                            res[counter]["status"] = "{0}".format(cur.statusmessage)

        finally:
            return res



