package-transformation-project:
	cd transformation && zip -r ../transformation.zip ./*.py ./**/*.py

run-prefect-worker:
	docker compose -f orchestration/compose.prefect-worker.yml run prefect-worker