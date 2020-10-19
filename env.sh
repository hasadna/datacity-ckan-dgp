#!/bin/bash

export PUBLIC_KEY="$(echo "${PUBLIC_KEY_B64}" | base64 -d)"
export PRIVATE_KEY="$(echo "${PRIVATE_KEY_B64}" | base64 -d)"
