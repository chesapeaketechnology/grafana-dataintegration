output "gfi_fe_fqdn" {
  value = azurerm_container_group.gfi_fe_container_group.fqdn
}

output "gfi_fe_ip_address" {
  value = azurerm_container_group.gfi_fe_container_group.ip_address
}

output "gfi_be_fqdn" {
  value = azurerm_container_group.gfi_be_container_group.fqdn
}

output "gfi_be_ip_address" {
  value = azurerm_container_group.gfi_be_container_group.ip_address
}
