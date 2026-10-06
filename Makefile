# Every way to run, test and poke the service. `make help` lists the targets.
#
# Variables you can override on the command line:
#   HOST     the broker a curl target talks to          (default localhost:8080)
#   QUEUE    the queue the curl targets use             (default orders)
#   URLS     the brokers the harness loads, comma-separated
#   BROKER   the compose service to restart             (default broker-1)
#   ID       the message to acknowledge                 (default 0-0)

HOST   ?= localhost:8080
QUEUE  ?= orders
URLS   ?= http://localhost:8080
BROKER ?= broker-1
ID     ?= 0-0

CLUSTER_URLS = http://localhost:8081,http://localhost:8082,http://localhost:8083
JSON = -H 'Content-Type: application/json'

.DEFAULT_GOAL := help

.PHONY: help build test run harness \
        cluster cluster-harness cluster-restart cluster-logs cluster-down monitoring \
        create-queue describe enqueue dequeue ack metrics dlq

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{ printf "  %-18s %s\n", $$1, $$2 }'

## Build and test (JDK 17)

build: ## Compile, test and package the jar
	./gradlew build

test: ## Run the tests; the etcd tests are skipped when Docker is not running
	./gradlew test

## One broker

run: ## Start one broker on port 8080 with the in-memory coordinator
	./gradlew bootRun

harness: ## Load the broker(s) in URLS with producers and consumers and check nothing was lost
	./gradlew harness --args="--urls=$(URLS)"

## A cluster (Docker): etcd and three brokers on ports 8081, 8082 and 8083

cluster: ## Build the image and start etcd and three brokers in the background
	docker compose up --build -d

cluster-harness: ## Load all three brokers on a queue replicated three ways and kill a leader half way
	./gradlew harness --args="--urls=$(CLUSTER_URLS) --replicationFactor=3 --minInSyncReplicas=2 --skipAckPercent=0 --killCommand='docker compose kill {leader}'"

cluster-restart: ## Bring BROKER back; it catches up and rejoins as a follower
	docker compose up -d $(BROKER)

cluster-logs: ## Follow the brokers' logs
	docker compose logs -f broker-1 broker-2 broker-3

monitoring: ## Also start Prometheus on port 9090, scraping all three brokers
	docker compose --profile monitoring up -d

cluster-down: ## Stop and remove the cluster
	docker compose --profile monitoring down

## By hand, with curl, against HOST and QUEUE

create-queue: ## Create QUEUE with default settings
	curl -s -X POST $(HOST)/queues $(JSON) -d '{"name":"$(QUEUE)"}'; echo

describe: ## Show QUEUE's settings and, per shard, its leader, epoch, replicas and in-sync replicas
	curl -s $(HOST)/queues/$(QUEUE); echo

enqueue: ## Enqueue a HIGH priority message
	curl -s -X POST $(HOST)/queues/$(QUEUE)/messages $(JSON) -d '{"payload":"hello","priority":"HIGH"}'; echo

dequeue: ## Dequeue the highest-priority ready message
	curl -s -X POST $(HOST)/queues/$(QUEUE)/messages/dequeue; echo

ack: ## Acknowledge message ID
	curl -s -o /dev/null -w '%{http_code}\n' -X POST $(HOST)/queues/$(QUEUE)/messages/$(ID)/ack

metrics: ## QUEUE's metrics as JSON
	curl -s $(HOST)/queues/$(QUEUE)/metrics; echo

dlq: ## QUEUE's dead letters
	curl -s $(HOST)/queues/$(QUEUE)/dlq; echo
