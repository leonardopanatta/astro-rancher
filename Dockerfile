FROM quay.io/astronomer/astro-runtime:8.2.0

ENV AIRFLOW_VAR_MY_DAG_PARTNER='{"name":"partner_a","api_secret":"mysecret","path":"/tmp/partner_a"}'