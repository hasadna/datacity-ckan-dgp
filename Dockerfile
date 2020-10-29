FROM akariv/dgp-app:844c2fb4ad6ab5b47696e2eb9d40548ef34e621d

USER root
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY taxonomies taxonomies
COPY configuration.template.json .
COPY datacity_ckan_entrypoint.sh .
COPY env.sh .
COPY render_configuration_template.py .
COPY operator_ckan_sync.py dags/operators/ckan_sync/__init__.py
COPY setup.py /datacity-ckan-dgp/
COPY datacity_ckan_dgp /datacity-ckan-dgp/datacity_ckan_dgp
RUN pip install -e /datacity-ckan-dgp
USER etl

ENV AIRFLOW__CORE__PARALLELISM=1
ENV AIRFLOW__WEBSERVER__WORKERS=1

ENTRYPOINT [ "/app/datacity_ckan_entrypoint.sh" ]
