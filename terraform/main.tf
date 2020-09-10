terraform {
  required_version = "~> 0.12"
  experiments      = [variable_validation]
}

provider "azurerm" {
  version = "~> 2.18.0"
  features {}
  disable_terraform_partner_id = true
}

data "azurerm_resource_group" "gfi_resource_group" {
  name = var.resource_group_name
}

resource "azurerm_storage_account" "gfi_storage_account" {
  name                     = join("", ["sa", var.system_name, var.environment, "gfi"])
  resource_group_name      = data.azurerm_resource_group.gfi_resource_group.name
  location                 = data.azurerm_resource_group.gfi_resource_group.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "gfi_storage_container" {
  name                  = join("-", ["sc", var.system_name, var.environment, "gfi-checkpoint"])
  storage_account_name  = azurerm_storage_account.gfi_storage_account.name
  container_access_type = "private"
}

locals {
  max_cg_cpu = 2
  max_cg_mem = 8
  c_cpu = local.max_cg_cpu / length(var.topic_settings.topics)
  c_mem = local.max_cg_mem / length(var.topic_settings.topics)
  s_cpu = local.max_cg_cpu / length(var.system_topic_settings.topics)
  s_mem = local.max_cg_mem / length(var.system_topic_settings.topics)
}

# Create a Container Group
resource "azurerm_container_group" "gfi_fe_container_group" {
  depends_on          = [var.topic_settings]
  name                = join("-", [var.system_name, var.environment, "grafana-fe-topic-integration"])
  resource_group_name = data.azurerm_resource_group.gfi_resource_group.name
  location            = data.azurerm_resource_group.gfi_resource_group.location
  ip_address_type     = "private"
  network_profile_id  = var.network_profile_id
  os_type             = "Linux"

  tags = var.default_tags

//  #Consul container for the group
//  container {
//    name = "gfi-consul"
//    image = "consul:1.8"
//    cpu = "1"
//    memory = "1"
//    commands=["consul", "agent", "-retry-join=${var.consul_server}"]
//
//    # Gossip protocol between agents and servers
//    ports {
//      port     = 8301
//      protocol = "TCP"
//    }
//
//    # RPC for CLI tools
//    ports {
//      port     = 8400
//      protocol = "TCP"
//    }
//
//    # HTTP API
//    ports {
//      port     = 8500
//      protocol = "TCP"
//    }
//
//    # DNS Interface
//    ports {
//      port     = 8600
//      protocol = "TCP"
//    }
//
//    environment_variables = {
//      CONSUL_LOCAL_CONFIG="{\"leave_on_terminate\": true}"
//    }
//  }

  # Grafana Server
  dynamic container {
    for_each = var.topic_settings.topics
    content {
      name = join("-", ["gfi", replace(container.value, "_", "-"), "consumer"])
      image = "chesapeaketechnology/grafana-dataintegration:0.2.6"
      cpu = tonumber(format("%.2f", local.c_cpu - 0.01))
      memory = tonumber(format("%.1f", local.c_mem - 0.1))

      ports {
        port     = (3000 + index(tolist(var.topic_settings.topics), container.key))
        protocol = "TCP"
      }

      environment_variables = {
        GDI_TOPIC = container.value,
        GDI_CONSUMER_GROUP = "frontend",
        GDI_KEY = element(matchkeys(var.topic_settings.eventhub_keys, var.topic_settings.topics, [container.value]), 0),
        GDI_NAMESPACE = var.topic_settings.eventhub_namespace,
        GDI_SHARED_ACCESS_POLICY = element(matchkeys(var.topic_settings.eventhub_shared_access_policies, var.topic_settings.topics, [container.value]), 0),
        GDI_DB_HOST = var.db_host,
        GDI_DB_PORT = var.db_port,
        GDI_DB_DATABASE = var.db_name,
        GDI_DB_USER = var.db_user,
        GDI_DB_PASSWORD = var.db_password,
        GDI_DB_SCHEMA = var.db_schema,
        GDI_BUFFER_SIZE = 50,
        GDI_LOG_LEVEL = "WARNING",
        GDI_MAX_BUFFER_TIME_IN_SEC = 20,
        GDI_MAX_TIME_TO_KEEP_DATA_IN_SEC = 604800,
        GDI_DATA_EVICT_INTERVAL_IN_SEC = 7200,
        GDI_CHECKPOINT_STORE_CONNECTION=azurerm_storage_account.gfi_storage_account.primary_blob_connection_string,
        GDI_CHECKPOINT_STORE_CONTAINER=azurerm_storage_container.gfi_storage_container.name
      }
    }
  }
}

# Create a Container Group for System Topics (backend only topics)
resource "azurerm_container_group" "gfi_be_container_group" {
  depends_on          = [var.system_topic_settings]
  name                = join("-", [var.system_name, var.environment, "grafana-be-topic-integration"])
  resource_group_name = data.azurerm_resource_group.gfi_resource_group.name
  location            = data.azurerm_resource_group.gfi_resource_group.location
  ip_address_type     = "private"
  network_profile_id  = var.network_profile_id
  os_type             = "Linux"

  tags = var.default_tags

  # Grafana Server
  dynamic container {
    for_each = var.system_topic_settings.topics
    content {
      name = join("-", ["gfi", replace(container.value, "_", "-"), "consumer"])
      image = "chesapeaketechnology/grafana-dataintegration:0.2.6"
      cpu = tonumber(format("%.2f", local.s_cpu - 0.01))
      memory = tonumber(format("%.1f", local.s_mem - 0.1))

      ports {
        port     = (3000 + index(tolist(var.system_topic_settings.topics), container.key))
        protocol = "TCP"
      }

      environment_variables = {
        GDI_TOPIC = container.value,
        GDI_CONSUMER_GROUP = "frontend",
        GDI_KEY = element(matchkeys(var.system_topic_settings.eventhub_keys, var.system_topic_settings.topics, [container.value]), 0),
        GDI_NAMESPACE = var.system_topic_settings.eventhub_namespace,
        GDI_SHARED_ACCESS_POLICY = element(matchkeys(var.system_topic_settings.eventhub_shared_access_policies, var.system_topic_settings.topics, [container.value]), 0),
        GDI_DB_HOST = var.db_host,
        GDI_DB_PORT = var.db_port,
        GDI_DB_DATABASE = var.db_name,
        GDI_DB_USER = var.db_user,
        GDI_DB_PASSWORD = var.db_password,
        GDI_DB_SCHEMA = var.db_schema,
        GDI_BUFFER_SIZE = 50,
        GDI_LOG_LEVEL = "WARNING",
        GDI_MAX_BUFFER_TIME_IN_SEC = 20,
        GDI_MAX_TIME_TO_KEEP_DATA_IN_SEC = 604800,
        GDI_DATA_EVICT_INTERVAL_IN_SEC = 7200,
        GDI_CHECKPOINT_STORE_CONNECTION=azurerm_storage_account.gfi_storage_account.primary_blob_connection_string,
        GDI_CHECKPOINT_STORE_CONTAINER=azurerm_storage_container.gfi_storage_container.name
      }
    }
  }
}
