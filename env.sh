#!/bin/bash

[ "${PUBLIC_KEY_B64}" != "" ] && export PUBLIC_KEY="$(echo "${PUBLIC_KEY_B64}" | base64 -d)"
[ "${PRIVATE_KEY_B64}" != "" ] && export PRIVATE_KEY="$(echo "${PRIVATE_KEY_B64}" | base64 -d)"
