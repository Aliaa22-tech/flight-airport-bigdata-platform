from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

CH = "docker exec clickhouse clickhouse-client --user admin --password admin123"

SPARK_SUBMIT = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.host=spark-master "
    "--conf spark.jars.ivy=/tmp/.ivy "
    "--packages com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.3.1 "
    "/jobs/load_clickhouse.py"
)

SPARK_ANALYTICS = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.driver.host=spark-master "
    "--conf spark.jars.ivy=/tmp/.ivy "
    "--packages com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.3.1 "
    "/jobs/analytics_job.py"
)

with DAG(
    dag_id="flight_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["flight"],
) as dag:

    upload_to_hdfs = BashOperator(
        task_id="upload_to_hdfs",
        bash_command=(
            "docker exec namenode hdfs dfs -mkdir -p /flight/raw && "
            "for y in 2024 2025; do "
            "docker exec namenode hdfs dfs -test -d /flight/raw/$y || "
            "docker exec namenode hdfs dfs -put /data/$y /flight/raw/; "
            "done"
        ),
    )

    create_table = BashOperator(
        task_id="create_table",
        bash_command=(
            "cat /opt/airflow/dags/create_table.sql | "
            "docker exec -i clickhouse clickhouse-client "
            "--user admin --password admin123 --multiquery"
        ),
    )

    truncate_table = BashOperator(
        task_id="truncate_table",
        bash_command=CH + ' --query "TRUNCATE TABLE flights.flights"',
    )

    spark_load = BashOperator(
        task_id="spark_load",
        bash_command=SPARK_SUBMIT,
    )

    validate_count = BashOperator(
        task_id="validate_count",
        bash_command=(
            "cnt=$(" + CH + ' --query "SELECT count() FROM flights.flights"); '
            'echo "ROWS IN CLICKHOUSE: $cnt"; '
            'test "$cnt" -eq 14080680'
        ),
    )

    create_agg_tables = BashOperator(
        task_id="create_agg_tables",
        bash_command=(
            "docker exec spark-master cat /jobs/create_agg_tables.sql | "
            "docker exec -i clickhouse clickhouse-client "
            "--user admin --password admin123 --multiquery"
        ),
    )

    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=SPARK_ANALYTICS,
    )

    upload_to_hdfs >> create_table >> truncate_table >> spark_load >> validate_count >> create_agg_tables >> spark_analytics