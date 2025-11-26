package-transformation-project:
	cd transformation && zip -r ../transformation.zip ./*.py ./**/*.py

run-prefect-worker:
	docker compose -f orchestration/compose.prefect-worker.yml up --build

pytest-transformation:
	docker build -f Dockerfile.pytest -t pyspark-pytest . && \
	docker run --rm --name pytest-transformation \
	--entrypoint /bin/bash \
	pyspark-pytest \
	-c 'pytest -v tests/'