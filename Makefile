setup-hadoop-cluster:
	docker compose -f cluster/compose.cluster.yml up -d

setup-prefect-server:
	docker compose -f cluster/compose.prefect.yml up -d

run-spark-history-server:
	docker compose -f cluster/compose.spark.yml up spark-history-server -d

setup-jupyter-server:
	docker compose -f cluster/compose.jupyter.yml up -d --build

setup-hive-server:
	docker compose -f cluster/compose.hive.yml up -d