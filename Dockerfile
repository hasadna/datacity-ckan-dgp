# Pulled Jun 20, 2024
FROM akariv/dgp-app@sha256:58fdcb67d72e337818b9a0d67d6705ae95e215c0d0d53202565f577f56f89d91

USER root
RUN apt-get update && apt-get install -y git zip libgdal-dev build-essential
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY taxonomies taxonomies
COPY configuration.template.json .
COPY datacity_ckan_entrypoint.sh .
COPY env.sh .
COPY render_configuration_template.py .
COPY setup.py /datacity-ckan-dgp/
COPY datacity_ckan_dgp /datacity-ckan-dgp/datacity_ckan_dgp
COPY create_operator_dags.sh .
RUN pip install -e /datacity-ckan-dgp &&\
    ./create_operator_dags.sh /datacity-ckan-dgp/datacity_ckan_dgp/operators dags/operators
USER etl

ENV AIRFLOW__CORE__PARALLELISM=1
ENV AIRFLOW__WEBSERVER__WORKERS=1

ENTRYPOINT [ "/app/datacity_ckan_entrypoint.sh" ]
