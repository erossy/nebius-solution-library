resource "nebius_compute_v1_instance" "s3-test-instance" {
  parent_id = var.parent_id
  name      = "s3-test-instance"

  boot_disk = {
    attach_mode   = "READ_WRITE"
    existing_disk = nebius_compute_v1_disk.s3-test-boot-disk
  }

  network_interfaces = [
    {
      name       = "eth0"
      subnet_id  = var.subnet_id
      ip_address = {}
#      public_ip_address = {}
    }
  ]

  resources = {
    platform = local.platform
    preset   = local.preset
  }


  cloud_init_user_data = templatefile("templates/s3-uploader-cloud-init.tftpl", {
    ssh_user_name  = var.ssh_user_name,
    ssh_public_key = local.ssh_public_key,
  })
}

resource "nebius_compute_v1_instance" "jh-instance" {
  parent_id = var.parent_id
  name      = "s3-test-jh"

  boot_disk = {
    attach_mode   = "READ_WRITE"
    existing_disk = nebius_compute_v1_disk.s3-jh-boot-disk
  }

  network_interfaces = [
    {
      name       = "eth0"
      subnet_id  = var.subnet_id
      ip_address = {}
      public_ip_address = {}
    }
  ]

  resources = {
    platform = "cpu-d3"
    preset   = "4vcpu-16gb"
  }


  cloud_init_user_data = templatefile("templates/s3-uploader-cloud-init.tftpl", {
    ssh_user_name  = var.ssh_user_name,
    ssh_public_key = local.ssh_public_key,
  })
}
