resource "nebius_compute_v1_instance" "bastion_instance" {
  parent_id = var.parent_id
  name      = "bastion-instance"

  boot_disk = {
    attach_mode   = "READ_WRITE"
    existing_disk = nebius_compute_v1_disk.bastion-boot-disk
  }

  network_interfaces = [
    {
      name              = "eth0"
      subnet_id         = var.subnet_id
      ip_address        = {}
      public_ip_address = {}
    }
  ]

  resources = {
    platform = "cpu-e2"
    preset   = "4vcpu-16gb"
  }

  service_account_id = nebius_iam_v1_service_account.bastion-sa.id

  cloud_init_user_data = templatefile("../modules/cloud-init/bastion-cloud-init.tftpl", {
    ssh_user_name      = var.ssh_user_name
    ssh_public_key     = local.ssh_public_key
    parent_id          = var.parent_id
    service_account_id = local.service_account_id
  })
}