#!/bin/bash

if [ "${PUBLIC_KEY_B64}" != "" ]; then
  export PUBLIC_KEY="$(echo "${PUBLIC_KEY_B64}" | base64 -d)"
fi &&\
if [ "${PRIVATE_KEY_B64}" != "" ]; then
  export PRIVATE_KEY="$(echo "${PRIVATE_KEY_B64}" | base64 -d)"
fi
