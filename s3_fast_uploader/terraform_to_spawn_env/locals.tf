locals {
  ssh_public_key = var.ssh_public_key.key != null ? var.ssh_public_key.key : (
  fileexists(var.ssh_public_key.path) ? file(var.ssh_public_key.path) : null)

  regions_default = {
    eu-west1 = {
      platform = "cpu-d3"
      preset   = "128vcpu-512gb"
    }
    eu-north1 = {
      platform = "gpu-h100-sxm"
      preset   = "8gpu-128vcpu-1600gb"
    }

    eu-north2 = {
      platform = "cpu-d3"
      preset   = "128vcpu-512gb"
    }
  }

  current_region_defaults = local.regions_default[var.region]

  platform = coalesce(var.platform, local.current_region_defaults.platform)
  preset   = coalesce(var.preset, local.current_region_defaults.preset)

}
