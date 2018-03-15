# Postgresql-tools

Postgresql-tools contains the:
- container tests (**/tests**)
- service tests (**/tests**)
- lib to execute sql scrips (**/postgresql_lib**)
- sql scripts needed for the postgresql container (**/scripts**)


## Launch container tests

The folder **test_config** contains the configuration parameters needed to run the tests.

```
python -m pytest tests/test_postgres_container.py -vs --config-file test_config/dev.json --psql-config-file test_config/psql.json 

```

The paremeter **-v** and **-s** are used to increase the verbosity.  

## Launch service tests
```
python -m pytest tests/test_postgres_service.py -vs --config-file test_config/dev.json --psql-config-file test_config/psql.json 

```

