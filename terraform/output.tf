output "gfi_fqdn" {
  value = azurerm_container_group.gfi_container_group.fqdn
}

output "gfi_ip_address" {
  value = azurerm_container_group.gfi_container_group.ip_address
}
