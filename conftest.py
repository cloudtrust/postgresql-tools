#!/usr/bin/env python
# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import pytest
import json

def pytest_addoption(parser):
	parser.addoption("--config-file", action="store", help="Json container configuration file ", dest="config_file")
	parser.addoption("--psql-config-file", action="store", help="Json psql credentials file ", dest="psql_config_file")


@pytest.fixture()
def settings(pytestconfig):
	try:
		with open(pytestconfig.getoption('config_file')) as json_data:
			config = json.load(json_data)

	except IOError as e:
		raise IOError("Config file {path} not found".format(path=pytestconfig.getoption('config_file')))

	return config

@pytest.fixture()
def psql_settings(pytestconfig):
	try:
		with open(pytestconfig.getoption('psql_config_file')) as json_data:
			config = json.load(json_data)

	except IOError as e:
		raise IOError("Psql config file {path} not found".format(path=pytestconfig.getoption('psql_config_file')))

	return config