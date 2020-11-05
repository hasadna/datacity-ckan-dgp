FROM akariv/dgp-app:b8de56cf4ed82722a891b1de735f2bbb34dfc9fb

USER root
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
