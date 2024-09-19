#!/bin/bash
set -e

# Run pytest tests
pytest

# Run dbt tests
cd ./dbt_dreamsdata/
dbt run
dbt test
