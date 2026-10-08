\# Flight \& Airport Big Data Analytics Platform



A big data platform that analyzes flight operations, delays, cancellations and airport performance using the BTS On-Time Performance dataset (2024-2025), with both a batch path and a real-time path. Everything runs in Docker.



\## Architecture



\*\*Batch (historical data)\*\*

BTS CSV files -> Airflow -> HDFS -> Spark (ETL + analytics) -> ClickHouse -> Power BI



\*\*Real-time\*\*

Python producer -> Kafka -> Spark Structured Streaming -> ClickHouse -> Grafana (dashboards + alert)



Both paths share ClickHouse as the analytics layer.



\## Dataset



\- Source: BTS On-Time Performance (Bureau of Transportation Statistics)

\- Period: 2024-2025 (24 monthly CSV files)

\- Size: 14,080,680 flights, about 5.9 GB, 109 fields

\- The dataset is NOT included in this repository (too large for GitHub).



\## Requirements



\- Windows 10/11 with Docker Desktop (WSL2 backend)

\- 16 GB RAM recommended

\- 40-50 GB free disk space



\## Setup



1\. Clone the repository:

```

&#x20;  git clone <REPO\_URL>

&#x20;  cd <REPO\_FOLDER>

```

2\. Download the 24 monthly CSV files from the BTS website and place them like this:

```

&#x20;  data/2024/2024\_1.csv ... data/2024/2024\_12.csv

&#x20;  data/2025/2025\_1.csv ... data/2025/2025\_12.csv

```

&#x20;  (The real-time producer reads `data/2025/2025\_12.csv`.)

3\. Start the containers:

```

&#x20;  docker compose up -d --build

```



\## Running the batch pipeline



1\. Open Airflow at http://localhost:8085

2\. Trigger the DAG `flight\_pipeline`. Its tasks run in this order:

&#x20;  `upload\_to\_hdfs -> create\_table -> truncate\_table -> spark\_load -> validate\_count -> create\_agg\_tables -> spark\_analytics`

3\. When it finishes, ClickHouse contains `flights.flights` (14,080,680 rows) and the aggregation tables (`agg\_airline`, `agg\_airport`, `agg\_hour`, `agg\_month`, `agg\_dayofweek`, `agg\_route`, `agg\_delay\_cause`).



\## Running the real-time pipeline



1\. Make sure the `producer` and `kafka` containers are running.

2\. Start the Spark streaming job:

```

&#x20;  docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --conf spark.jars.ivy=/tmp/.ivy --conf spark.cores.max=1 --packages com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.3.1,org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.3 /jobs/live\_stream.py

```

3\. Open Grafana at http://localhost:3000 and the dashboard "Live Flights".



\## Web interfaces



| Tool | URL |

|---|---|

| Airflow | http://localhost:8085 |

| HDFS NameNode | http://localhost:9870 |

| Spark Master | http://localhost:8080 |

| Grafana | http://localhost:3000 |

| ClickHouse (HTTP) | http://localhost:8123 |



\## Dashboards



\- \*\*Power BI\*\* (historical analysis): connects to ClickHouse and reads the `agg\_\*` tables.

\- \*\*Grafana\*\* (real-time): live KPIs, delay rate by airline, flight status distribution and a high-delay-rate alert.
### Restoring the Grafana dashboard and alert

The exported files are in the `grafana/` folder.

1. Open Grafana at http://localhost:3000 and add a ClickHouse data source (Connections -> Add new connection -> ClickHouse). Use host `clickhouse`, port `8123` (HTTP), user `admin`, password `admin123`, database `flights`.
2. Dashboard: Dashboards -> New -> Import -> upload `grafana/live-flights-dashboard.json`, then pick the ClickHouse data source you just created.
3. Alert: Alerting -> Alert rules -> Import (or New alert rule) and use `grafana/high-delay-alert.yaml` as the reference.



\## Project structure



```

airflow/      Airflow image (Dockerfile)

dags/         Airflow DAG (flight\_pipeline.py) and SQL files

jobs/         Spark jobs and ClickHouse table definitions

producer/     Python Kafka producer that simulates live flight events

data/         Dataset (not in the repo)

docker-compose.yml

```



\## Machine learning (in progress)



A Spark MLlib model for predicting flight delays. It reads from `flights.flights` and writes results to `flights.predictions`.



\## Notes



The credentials in this repository (`admin` / `admin123`) are for local development only.

