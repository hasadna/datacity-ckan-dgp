FROM akariv/dgp-app:a4b48191b99a39068ec4f2c98a578d51add17d42

USER root
RUN apt-get update && apt-get install -y git
COPY requirements.txt .
RUN pip install -r requirements.txt
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
