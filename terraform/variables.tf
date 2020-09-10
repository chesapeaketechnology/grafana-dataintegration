variable "resource_group_name" {
  type        = string
  description = "Azure resource group in which to deploy"
}

variable "virtual_network_name" {
  type        = string
  description = "Name of Azure virtual network"
}

variable "location" {
  type        = string
  description = "Region to provision resources in"
}

variable "system_name" {
  type        = string
  description = "Name to use for the data science cluster being created"
}

variable "environment" {
  type        = string
  description = "Current Environment to provision within"
}

variable "default_tags" {
  type        = map(string)
  description = "Collection of default tags to apply to all resources"
}

variable "network_profile_id" {
  type        = string
  description = "Name of the network profile in which to create the container"
}

variable "db_host" {
  type        = string
  description = "The database fully qualified host name"
}
variable "db_port" {
  type        = string
  description = "The database connection port"
  default     = "5432"
}
variable "db_name" {
  type        = string
  description = "The database name for the database connection"
}
variable "db_user" {
  type        = string
  description = "The username for the database connection"
}
variable "db_password" {
  type        = string
  description = "The password for the database connection"
}
variable "db_schema" {
  type        = string
  description = "The database schema being used for storing data points."
  default     = "public"
}

variable "topics"{
  type        = set(string)
  description = "List of eventhubs to create under this eventhubs space"
}

variable "system_topics" {
  type        = list(object({
    topic                               = string
    eventhub_namespace                  = string
    eventhub_primary_key                = string
    eventhub_shared_access_policy_name  = string
  }))
}

variable "eventhub_keys" {
  type        = list(string)
  description = "The access keys for eventhub; ordered the same as the topics."
}
variable "eventhub_namespace" {
  type        = string
  description = "The fully qualified eventhub namespace"
}
variable "eventhub_shared_access_policies" {
  type        = list(string)
  description = "A list of names of the eventhub shared access policy used to authenticate to eventhub; ordered the same as the topics."
}

variable "checkpoint_store_connection_str" {
  type        = string
  description = "[Optional] The azure blob container connection string is checkpoiint storage is being used."
  default     = ""
}
variable "checkpoint_store_container" {
  type        = string
  description = "[Optional] The azure blob container to be used for eventhub checkpoint storage."
  default     = ""
}

variable "consul_server" {
  type        = string
  description = "IP address of a Consul server to join"
}
