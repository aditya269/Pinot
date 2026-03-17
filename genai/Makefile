include ../Makefile

.PHONY: clients infra topics tables recipe loader question validate student_topics student_tables student_recipe student_loader student_question student_exam_topics student_exam_tables student_exam_recipe student_exam_loader student_fees_topics student_fees_tables student_fees_recipe student_fees_loader cdr_topics cdr_tables cdr_offline_tables cdr_hybrid_tables cdr_recipe cdr_hybrid_recipe cdr_loader cdr_offline_loader

URL=https://www.rtasummit.com/agenda
QUERY='when and where does kishore speak in rtasummit'
STUDENT_CSV=/code/data/students.csv
STUDENT_EXAM_CSV=/code/data/student_exams.csv
STUDENT_FEES_CSV=/code/data/student_fees.csv
CDR_CSV=/code/data/cdr_data.csv
CDR_OFFLINE_CSV=/code/data/cdr_data.csv

clients:
	docker build docker -t startree/genai

infra: clients helper
	docker compose \
		-f ../pinot-compose.yml \
		-f ../kafka-compose.yml \
		up -d

topics:
	-@docker exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--topic documentation

student_topics:
	-@docker exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--topic student

student_exam_topics:
	-@docker exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--topic student_exam

student_fees_topics:
	-@docker exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--topic student_fees

cdr_topics:
	-@docker exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--topic cdr_data

tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-tableConfigFile config/table.json \
		-schemaFile config/schema.json \
		-exec

	@TABLE=documentation make check_table

student_tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-tableConfigFile config/student_table.json \
		-schemaFile config/student_schema.json \
		-exec

	@TABLE=student make check_table

student_exam_tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-tableConfigFile config/student_exam_table.json \
		-schemaFile config/student_exam_schema.json \
		-exec

	@TABLE=student_exam make check_table

student_fees_tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-tableConfigFile config/student_fees_table.json \
		-schemaFile config/student_fees_schema.json \
		-exec

	@TABLE=student_fees make check_table

cdr_tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-tableConfigFile config/cdr_table.json \
		-schemaFile config/cdr_schema.json \
		-exec

	@TABLE=cdr_data make check_table

cdr_offline_tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-tableConfigFile config/cdr_offline_table.json \
		-schemaFile config/cdr_schema.json \
		-exec

	@TABLE=cdr_data make check_table

cdr_hybrid_tables:
	@docker cp config/ pinot-controller:/opt/pinot/

	@docker exec pinot-controller ./bin/pinot-admin.sh \
		AddTable \
		-offlineTableConfigFile config/cdr_offline_table.json \
		-realtimeTableConfigFile config/cdr_table.json \
		-schemaFile config/cdr_schema.json \
		-exec

	@TABLE=cdr_data make check_table

recipe: infra check topics tables

student_recipe: infra check student_topics student_tables

student_exam_recipe: infra check student_exam_topics student_exam_tables

student_fees_recipe: infra check student_fees_topics student_fees_tables

cdr_recipe: infra check cdr_topics cdr_tables

cdr_hybrid_recipe: infra check cdr_topics cdr_hybrid_tables

loader: clients
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		--volume $(shell pwd)/config:/code/config \
		-e OPENAI_API_KEY=${OPENAI_API_KEY} \
		-e MAX_DEPTH=${MAX_DEPTH} \
		startree/genai \
		python loader.py ${URL}

student_loader: clients
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		--volume $(shell pwd):/code \
		startree/genai \
		python student_loader.py ${STUDENT_CSV}

student_exam_loader: clients
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		--volume $(shell pwd):/code \
		startree/genai \
		python student_exam_loader.py ${STUDENT_EXAM_CSV}

student_fees_loader: clients
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		--volume $(shell pwd):/code \
		startree/genai \
		python student_fees_loader.py ${STUDENT_FEES_CSV}

cdr_loader: clients
	@docker run -it \
		--network $(DOCKER_NETWORK) \
		--volume $(shell pwd):/code \
		startree/genai \
		python cdr_loader.py ${CDR_CSV}

cdr_offline_loader:
	@docker cp config/cdr_offline_job-spec.yml pinot-controller:/opt/pinot/
	@docker exec pinot-controller mkdir -p /input/
	@docker exec pinot-controller mkdir -p /data/cdr_data_offline_segments/
	@docker cp $(CDR_OFFLINE_CSV) pinot-controller:/input/

	@docker exec pinot-controller \
		./bin/pinot-admin.sh LaunchDataIngestionJob \
		-jobSpecFile cdr_offline_job-spec.yml

question: clients
	@docker run -it \
	 	--network $(DOCKER_NETWORK) \
		-e OPENAI_API_KEY=${OPENAI_API_KEY} \
		startree/genai \
		python genai.py ${QUERY}

student_question: clients
	@docker run -it \
	 	--network $(DOCKER_NETWORK) \
		--volume $(shell pwd)/output:/code/output \
		-e OPENAI_API_KEY=${OPENAI_API_KEY} \
		-e QUERY_MODE=student \
		-e OUTPUT_DIR=/code/output \
		startree/genai \
		python genai.py


validate: loader
	@echo 'passed'
