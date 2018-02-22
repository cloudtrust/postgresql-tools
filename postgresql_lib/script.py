#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#



import psycopg2
import sys
import logging

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.postgresql_lib.script")
logger.setLevel(logging.DEBUG)

class PostgresqlScriptExecutor(object):
    @staticmethod
    def run(con, script, rollback_script):
        """

        :param con: connection to postgresql
        :param script: list of queries to execute
        :return: transcript of the executed queries
        """
        res = ""
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with con:
                with con.cursor() as cur:
                    # execute the sql script query by query
                    list_commands = script.split(";\n")
                    counter = 1
                    for command in list_commands:
                        if command and not command.isspace():
                            res +=  "{0} : {1} \n".format(counter, command)
                            cur.execute(command)
                            logger.info(command)
                            res += "{0} : {1} \n".format(counter, cur.statusmessage)
                            counter += 1
            con.commit()
        except Exception as e:
            logger.debug(e)
            if con:
                # execute the rollback script
                res += "Something went wrong: {0}; doing the rollback \n".format(e)
                with con.cursor() as cur:
                    list_commands = rollback_script.split(";\n")
                    counter = 1
                    for command in list_commands:
                        if command and not command.isspace():
                            res += "{0} : {1} \n".format(counter, command)
                            cur.execute(command)
                            logger.info(command)
                            res += "{0} : {1} \n".format(counter, cur.statusmessage)
                            counter += 1
        finally:
            return res



