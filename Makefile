PROJECT_NAME="grafana-dataintegration"
export PROJECT_NAME
TAG="0.1.0"
export TAG
ORG="chesapeaketechnology"
export ORG

## build : Build the docker images
build:
	docker build --no-cache -t ${ORG}/${PROJECT_NAME}:${TAG} .

## run : Run the docker image locally
run:
	 docker run -d --name ${PROJECT_NAME} -e LOG_LEVEL="DEBUG" -e GDI_TOPIC="lte_message" -e GDI_MESSAGE_TYPE="LteRecord" -e GDI_MESSAGE_VERSION="~=0.1.0" -e GDI_KEY="udVOb6iPmBlEjFiFJYtyT7yy/U5Fd5WGWxWZK2nGfLM=" -e GDI_NAMESPACE="datasci-dev-mqtt-eventhubs-namespace.servicebus.usgovcloudapi.net" -e GDI_SHARED_ACCESS_POLICY="LTE_MESSAGE-auth-rule" -e GDI_DB_HOST="docker.for.mac.host.internal" -e GDI_DB_PORT="5432" -e GDI_DB_DATABASE="snet" -e GDI_DB_USER="postgres" -e GDI_DB_PASSWORD="MonkeyDance" -e GDI_DB_SCHEMA="public" ${ORG}/${PROJECT_NAME}:${TAG}
#         -e GDI_CHECKPOINT_STORE_CONNECTION=
#         -e GDI_CHECKPOINT_STORE_CONTAINER=

## push : Push the docker repositories to docker hub
push:
	docker push ${ORG}/${PROJECT_NAME}:${TAG}

help : Makefile
	@sed -n 's/^##//p' $<

.PHONY: help build run push
