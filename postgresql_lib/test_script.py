#!/usr/bin/env python
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

import script
import pytest
import logging
import psycopg2

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("postgres_tools.postgresql_lib.test_script")
logger.setLevel(logging.INFO)


@pytest.mark.usefixtures('psql_settings', scope='class')
class TestScript():
    """Class to test the python script.py."""

    def test_script_working(self, psql_settings):
        """Test to check that the script does the changes we expect him to do."""

        script_create_user = "CREATE USER test_script;"
        user = "test_script"
        script_drop_user = "DROP USER test_script;"
        config = psql_settings

        try:
            logger.info("connecting to postgres with user {user}".format(user=config['user']))
            with psycopg2.connect(host=config['host'], user=config['user'],
                                   password=config['password']) as con:
                with con.cursor() as cur:
                    # create user
                    script.PostgresqlScriptExecutor().run(con, script_create_user)

                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=user))
                    assert cur.rowcount == 1

                    # drop user
                    script.PostgresqlScriptExecutor().run(con, script_drop_user)

                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=user))
                    assert cur.rowcount == 0

        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error {error}".format(error=e))
        finally:
            if con:
                con.close()
                logger.info("closed connection to postgresql")

    def test_rollback(self, psql_settings):
        """Test to check that the rollback works: if an error occurs, we undo all the modifications."""

        script_create_user = "CREATE USER test_script;\n CREATE invalid_syntax;"
        user = "test_script"
        script_drop_user = "DROP USER test_script;"
        config = psql_settings

        try:
            logger.info("Connecting to postgres with user {user}".format(user=config['user']))

            with psycopg2.connect(host=config['host'], user=config['user'], password=config['password']) as con:
                with con.cursor() as cur:
                    try:
                        print(script.PostgresqlScriptExecutor().run(con, script_create_user))
                    except Exception as e:
                        logger.info(e)
                        script.PostgresqlScriptExecutor().run(con, script_drop_user)

                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='{user}'".format(user=user))
                    assert cur.rowcount == 0

        except Exception as e:
            logger.debug(e)
            if con:
                con.rollback()
            pytest.fail("Error {error}".format(error=e))

        if con:
            con.close()
            logger.info("Closed connection to postgresql")

