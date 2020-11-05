#!/usr/bin/env bash

SOURCE_DIR="${1}"
TARGET_DIR="${2}"

mkdir -p "${TARGET_DIR}" &&\
for SOURCE_FILENAME in ${SOURCE_DIR}/*.py; do
  if cat "${SOURCE_FILENAME}" | grep "def operator(" >/dev/null; then
    SOURCE_BASENAME="$(basename -s ".py" "${SOURCE_FILENAME}")" &&\
    mkdir -p "${TARGET_DIR}/${SOURCE_BASENAME}" &&\
    echo "from datacity_ckan_dgp.operators import ${SOURCE_BASENAME}


def operator(name, params):
    ${SOURCE_BASENAME}.operator(name, params)" > "${TARGET_DIR}/${SOURCE_BASENAME}/__init__.py" &&\
    echo Created dag operator "${SOURCE_BASENAME}"
  fi
done