FROM postgres:12@sha256:a1e04460fdd3c338d6b65a2ab66b5aa2748eb18da3e55bcdc9ef17831ed3ad46

COPY 10-create-dbs.sql /docker-entrypoint-initdb.d/
COPY 20-readonly-user.sh /docker-entrypoint-initdb.d/

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
