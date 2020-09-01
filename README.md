# grafana-dataintegration
Integration between Grafana postgres frontend cache and the Data science 
pipeline.  

This is intended run as a docker container where one container listens 
to one topic. 

## Configuration
Per the [12 Factors](https://12factor.net/config) the configuration should be
in the environment.  

As a convenience for local execution, you may leverage the settings.yaml and .secrets.yaml
for providing configuration.  The values in these files will be used as defaults. Any
production (or docker) deployment should overrride these values using environment 
variables.

#### Environment Variables
##### Required
* GDI_TOPIC - EventHub topic name
* GDI_KEY - the EventHub access key
* GDI_NAMESPACE - the fully qualified namespace for the EventHub namespace
* GDI_SHARED_ACCESS_POLICY - The EventHub shared access policy name
* GDI_DB_HOST - The fully qualified host address for the integration database
* GDI_DB_PORT - The port for connecting to the integration database
* GDI_DB_DATABASE - The name of the database within the database server
* GDI_DB_USER - The user name to use when connecting to the database
* GDI_DB_PASSWORD - The password to use when connecting to the database
* GDI_DB_SCHEMA - The database schema
* GDI_CHECKPOINT_STORE_CONNECTION - The connection string for azure blob storage in which to store checkpoints.
* GDI_CHECKPOINT_STORE_CONTAINER - The azure blob storage container name in which to store checkpoints.

##### Optional
* GDI_CONSUMER_GROUP - The EventHub consumer group. Defaults to '$default'.
* GDI_BUFFER_SIZE - The number of messages to receive before writing to the database. Defaults to 1.
* LOG_LEVEL - Can be NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL. Defaults to ERROR. 
* MAX_BUFFER_TIME_IN_SEC - Maximum number of seconds between buffer flushes regardless of how many messages are in the buffer. Defaults to 20.
* MAX_TIME_TO_KEEP_DATA_IN_SEC - Maximum age of data kept in the database in seconds. Defaults to 7 days.
* DATA_EVICT_INTERVAL_IN_SEC - Frequency, in seconds, to evaluate, and evict, aged out data. Defaults to 2 hours. 


## Local Execution
### Setup
If you intend to run the project locally, you will need to have:
* Python 3.8
* Pipenv
* Make (usually part of existing dev tools)

1. To install python, it's recommended that you use [pyenv](https://github.com/pyenv/pyenv)
1. Once python 3.8 is installed, you need to install [pipenv](https://pipenv-fork.readthedocs.io/en/latest/).
1. If `make` is not installed please install your platform's development tools or gcc.
3. From the project directory run `pipenv install --dev`

### Run
To run it you can use the Makefile.  You will need to edit the environment variables for the run-local target.
```make run-local```

You could also simply run:
```python main.py```

*Be sure your are in your python virtualenv so that your libraries are on the path.*

## Docker Build & Execution
You will need to have docker and docker compose installed locally. See https://docs.docker.com/get-docker/ for more information.

#### Build/Re-Build the Image
To package code changes into the docker image, you must build it.  You can do this by running:
```make build```

#### Push the Image
To use the image for installation in the cloud, you will need to push the 
docker image to an accessible repository.  Currently, the Makefile is configured 
for hub.docker.com.

To use it, you will need to be logged into docker hub as a user with write access
to the Chesapeake organization.  If you are not logged in, try:
```docker login``` 

Once you are logged in via docker's cli tools you can run:
```make push```


## Terraform
The terraform files located in the terraform folder are used to deploy this integration into the datasci cloud.

To use the terraform, you will need to build and push any changes into the docker image. From their, you'll need 
to reference this terraform module from your main terraform script.  From example:

```
module "grafana-integration" {
  source               = "github.com/chesapeaketechnology/grafana-dataintegration/terraform"
  resource_group_name  = var.resource_group_name
  system_name          = var.cluster_name
  virtual_network_name = var.virtual_network_name
  location             = var.location
  environment          = var.environment
  default_tags         = var.default_tags
  network_profile_id   = var.network_profile_id
  db_host              = module.datasci-data.server_fqdn
  db_name              = "grafana"
  db_password          = module.datasci-data.administrator_password
  db_user              = "${module.datasci-data.administrator_login}@${module.datasci-data.server_name}"
  eventhub_namespace   = var.eventhub_namespace
  eventhub_keys        = var.eventhub_keys
  eventhub_shared_access_policies = var.eventhub_shared_access_policies
  topics               = var.topics
  consul_server        = var.consul_server
}
```


## Changelog

##### [0.2.4]() - 2020-08-20
* Resolved issue with db connection contention 

##### [0.2.3]() - 2020-08-20
* Added support for data eviction 

##### [0.2.2]() - 2020-08-19
* Resolved issue with identifying precision of unix time

##### [0.2.1]() - 2020-08-18
* Added support for max buffer time

##### [0.2.0]() - 2020-08-12
* Migrated to using a generic storage schema based on Postgres JSONB support
* Removed database migrations as they are no longer needed with the generic schema
* Removed message versioning as it is not longer needed with the generic schema
* Moved to using dynaconf for a layered configuration.
 
##### [0.1.0]() - 2020-08-12
* Initial build of integration code based on message type
* Added support for database migrations
* Added support for LteRecord Message type
* Added support for Message versioning

## Contact
* **Les Stroud** - [lstroud](https://github.com/lstroud)  
