provider "nebius" {
  domain = var.nebius_domain
  service_account = {
    private_key_file = var.nebius_private_key_file
    public_key_id    = var.nebius_public_key_id
    account_id       = var.nebius_account_id
  }
}
