#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

class PostgresqlScriptExecutor(object):
    @staticmethod
    def run(con, script):
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
                            res[counter]["command"] = "{command}".format(command=command)
                            cur.execute(command)
                            logger.info(command)
                            res[counter]["status"] = "{status}".format(status=cur.statusmessage)
            con.commit()
        except Exception as e:
            raise Exception("Unexpected failure when executing the script: {e}".format(e=e))

        return res

