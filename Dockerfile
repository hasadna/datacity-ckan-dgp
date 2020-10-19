FROM akariv/dgp-app:latest@sha256:57bbf219db5f20e0b721aac5a5b6dc4c2a4391ddd5ab3e6442f753abc50b0fe4

COPY taxonomies taxonomies
COPY configuration.template.json .
COPY datacity_ckan_entrypoint.sh .
COPY env.sh .
COPY render_configuration_template.py .
COPY operator_ckan_sync.py dags/operators/ckan_sync/__init__.py
COPY setup.py /datacity-ckan-dgp/
COPY datacity_ckan_dgp /datacity-ckan-dgp/datacity_ckan_dgp

USER root
RUN pip install -e /datacity-ckan-dgp

USER etl

ENV AIRFLOW__CORE__PARALLELISM=1
ENV AIRFLOW__WEBSERVER__WORKERS=1

ENTRYPOINT [ "/app/datacity_ckan_entrypoint.sh" ]
