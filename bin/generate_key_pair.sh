#!/usr/bin/env bash

openssl genrsa -out .tmpkey 2048
PRIVATE_KEY="$(openssl rsa -in .tmpkey -outform pem)"
PUBLIC_KEY="$(openssl rsa -in .tmpkey -outform pem -pubout)"
rm .tmpkey
echo "PRIVATE_KEY_B64=$(echo "${PRIVATE_KEY}" | base64 -w0)"
echo "PUBLIC_KEY_B64=$(echo "${PUBLIC_KEY}" | base64 -w0)"
