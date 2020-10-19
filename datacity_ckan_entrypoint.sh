#!/bin/bash

source env.sh

python3 render_configuration_template.py configuration.template.json > dags/configuration.json

/app/entrypoint.sh
