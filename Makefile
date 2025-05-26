.PHONY: submit-job build up down logs

# Variáveis
SPARK_MASTER=spark://spark-master:7077
CONTAINER_NAME=dsa-spark-master
JOBS_PATH=/opt/spark/apps
ARGS=$(filter-out $@,$(MAKECMDGOALS))
JOB=$(word 1,$(ARGS))
JOB_ARGS=$(wordlist 2,$(words $(ARGS)),$(ARGS))

# Comando base para spark-submit
define SPARK_SUBMIT
	docker exec -it $(CONTAINER_NAME) spark-submit \
		--master $(SPARK_MASTER) \
		--deploy-mode client
endef

# Construir containers
build:
	docker compose build

# Iniciar cluster
up:
	docker compose up -d --build --scale spark-worker=3

# Parar cluster
down:
	docker compose down

# Ver logs
logs:
	docker compose logs -f

# Executar job com argumentos
submit-job:
	@if [ -z "$(JOB)" ]; then \
		echo "Error: Please provide a job name. Usage: make submit-job job_name.py [args...]"; \
		exit 1; \
	fi
	$(SPARK_SUBMIT) $(JOBS_PATH)/$(JOB) $(JOB_ARGS)

# Catch-all target para argumentos
%:
	@:
