
export COMPOSE_PROJECT_NAME ?= recipes
DOCKER_NETWORK ?= $(COMPOSE_PROJECT_NAME)_default

helper:
	docker build ../docker/helper/ -t startree/helper

ifneq ("$(wildcard genai/Makefile)","")
student_recipe:
	$(MAKE) -C genai student_recipe

student_exam_recipe:
	$(MAKE) -C genai student_exam_recipe

student_fees_recipe:
	$(MAKE) -C genai student_fees_recipe

student_loader:
	$(MAKE) -C genai student_loader

student_exam_loader:
	$(MAKE) -C genai student_exam_loader

student_fees_loader:
	$(MAKE) -C genai student_fees_loader

student_question:
	$(MAKE) -C genai student_question
endif

check_kafka:
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		startree/helper \
		python helper.py kafka check broker

check_pinot:
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		startree/helper \
		python helper.py pinot check controller --sleep 10

check: check_pinot check_kafka

check_table:
	@# USAGE:
	@# TABLE=my_table make check_table
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		startree/helper \
		python helper.py pinot check table ${TABLE}

clean:
	@docker compose \
		-f ../kafka-compose.yml \
		-f ../pinot-compose.yml \
		-f ../postgres-compose.yml \
		-f ../pulsar-compose.yml \
		-f ../minio-compose.yml \
		-f ../flink-compose.yml \
		down

validate:
	@echo "VALIDATION HAS NOT BEEN IMPLEMENTED"

test: recipe validate clean

test_all:
	make test -C debezium-cdc/
	make test -C full-upserts/
	make test -C ingest-json-files-kafka/
	make test -C lookup-joins/
	make test -C startree-index/
	make test -C time-boundary-hybrid-table/
	make test -C managed-offline-flow/
	make test -C merge-small-segments/
	make test -C pulsar/
	make test -C minio-real-time/
	make test -C minio/
	make test -C genai/
	make test -C vector/
	make test -C video/
	
