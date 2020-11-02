FROM akariv/dgp-app:06d95c535a44537a59d28deeee4171698e6b56d0

USER root
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY taxonomies taxonomies
COPY configuration.template.json .
COPY datacity_ckan_entrypoint.sh .
COPY env.sh .
COPY render_configuration_template.py .
COPY operator_ckan_sync.py dags/operators/ckan_sync/__init__.py
COPY operator_datagov_fetcher.py dags/operators/datagov_fetcher/__init__.py
COPY setup.py /datacity-ckan-dgp/
COPY datacity_ckan_dgp /datacity-ckan-dgp/datacity_ckan_dgp
RUN pip install -e /datacity-ckan-dgp
USER etl

ENV AIRFLOW__CORE__PARALLELISM=1
ENV AIRFLOW__WEBSERVER__WORKERS=1

ENTRYPOINT [ "/app/datacity_ckan_entrypoint.sh" ]
