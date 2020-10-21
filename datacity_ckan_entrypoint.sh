#!/bin/bash

if ! psql "${DATABASE_URL}" -c 'select 1'; then
  echo waiting for DB &&\
  while ! psql "${DATABASE_URL}" -c 'select 1'; do
    echo . &&\
    sleep 1
  done
fi &&\
source env.sh &&\
python3 render_configuration_template.py configuration.template.json > dags/configuration.json &&\
exec /app/entrypoint.sh
