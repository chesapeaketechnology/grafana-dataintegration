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

//
//data "azurerm_virtual_network" "gfi_net" {
//  name                = var.virtual_network_name
//  resource_group_name = data.azurerm_resource_group.gfi_resource_group.name
//}

//# Create subnet for use with containers
//resource "azurerm_subnet" "gfi_subnet" {
//  name                 = "gfi_integration_subnet"
//  resource_group_name  = data.azurerm_resource_group.gfi_resource_group.name
//  virtual_network_name = data.azurerm_virtual_network.gfi_net.name
//  address_prefixes     = var.subnet_cidrs
//
//  delegation {
//    name = "gfi_integration_subnet_delegation"
//
//    service_delegation {
//      name = "Microsoft.ContainerInstance/containerGroups"
//      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
//    }
//  }
//}

//resource "azurerm_network_profile" "gfi_net_profile" {
//  name                = join("-", [var.system_name, var.environment, "gfi-net-profile"])
//  location            = data.azurerm_resource_group.gfi_resource_group.location
//  resource_group_name = data.azurerm_resource_group.gfi_resource_group.name
//
//  container_network_interface {
//    name = "container_nic"
//
//    ip_configuration {
//      name = "container_ip_config"
//      subnet_id = azurerm_subnet.gfi_subnet.id
//    }
//  }
//}

//resource "azurerm_storage_account" "gfi_storage_account" {
//  name                     = join("", ["sa", var.system_name, var.environment, "gfi"])
//  resource_group_name      = data.azurerm_resource_group.gfi_resource_group.name
//  location                 = data.azurerm_resource_group.gfi_resource_group.location
//  account_tier             = "Standard"
//  account_replication_type = "LRS"
//}
//
//resource "azurerm_storage_container" "gfi_storage_container" {
//  name                  = join("-", ["sc", var.system_name, var.environment, "gfi-checkpoint"])
//  storage_account_name  = azurerm_storage_account.gfi_storage_account.name
//  container_access_type = "private"
//}

//resource "azurerm_storage_blob" "example" {
//  name                   = "my-awesome-content.zip"
//  storage_account_name   = azurerm_storage_account.example.name
//  storage_container_name = azurerm_storage_container.example.name
//  type                   = "Block"
//  source                 = "some-local-file.zip"
//}

# Create a Container Group
resource "azurerm_container_group" "gfi_container_group" {
  depends_on          = [var.eventhub_keys, var.eventhub_shared_access_policies, var.eventhub_namespace]
  name                = join("-", [var.system_name, var.environment, "grafana-integration"])
  resource_group_name = data.azurerm_resource_group.gfi_resource_group.name
  location            = data.azurerm_resource_group.gfi_resource_group.location
  ip_address_type     = "private"
  network_profile_id  = var.network_profile_id
  os_type             = "Linux"

  tags = var.default_tags

  # Grafana Server
  dynamic container {
    for_each = var.topics
    content {
      name = join("-", ["gfi", replace(container.value, "_", "-"), "consumer"])
      image = "chesapeaketechnology/grafana-dataintegration:0.2.3"
      cpu = "1"
      memory = "2"

      ports {
        port     = 3000
        protocol = "TCP"
      }

      environment_variables = {
        GDI_TOPIC = container.value,
        GDI_CONSUMER_GROUP = "frontend",
        GDI_KEY = element(matchkeys(var.eventhub_keys, var.topics, [container.value]), 0),
        GDI_NAMESPACE = var.eventhub_namespace,
        GDI_SHARED_ACCESS_POLICY = element(matchkeys(var.eventhub_shared_access_policies, var.topics, [container.value]), 0),
        GDI_DB_HOST = var.db_host,
        GDI_DB_PORT = var.db_port,
        GDI_DB_DATABASE = var.db_name,
        GDI_DB_USER = var.db_user,
        GDI_DB_PASSWORD = var.db_password,
        GDI_DB_SCHEMA = var.db_schema,
        GDI_BUFFER_SIZE = 20,
        GDI_LOG_LEVEL = "INFO",
        MAX_BUFFER_TIME_IN_SEC = 20,
        MAX_TIME_TO_KEEP_DATA_IN_SEC = 604800,
        DATA_EVICT_INTERVAL_IN_SEC = 7200
//        GDI_CHECKPOINT_STORE_CONNECTION=azurerm_storage_account.gfi_storage_account.primary_blob_connection_string
//        GDI_CHECKPOINT_STORE_CONTAINER=azurerm_storage_container.gfi_storage_container.name
      }
    }
  }
}
