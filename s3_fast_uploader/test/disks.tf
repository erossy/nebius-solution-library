resource "nebius_compute_v1_disk" "s3-test-boot-disk" {
  parent_id           = var.parent_id
  name                = "s3-test-boot-disk"
  block_size_bytes    = 4096
  size_bytes          = 64424509440
  type                = "NETWORK_SSD"
  source_image_family = { image_family = "ubuntu22.04-driverless" }
}
