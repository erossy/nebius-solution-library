variable "parent_id" {
  description = "Project ID."
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID."
  type        = string
}

variable "ssh_public_key" {
  description = "SSH Public Key to access the cluster nodes"
  type = object({
    key  = optional(string),
    path = optional(string, "~/.ssh/id_rsa.pub")
  })
  default = {}
  validation {
    condition     = var.ssh_public_key.key != null || fileexists(var.ssh_public_key.path)
    error_message = "SSH Public Key must be set by `key` or file `path` ${var.ssh_public_key.path}"
  }
}

variable "nebius_domain" {
  description = "Nebius API domain"
  type        = string
  default     = "api.eu.nebius.cloud:443"
}

variable "nebius_private_key_file" {
  description = "Path to the private key file"
  type        = string
  sensitive   = true
}

variable "nebius_public_key_id" {
  description = "Public key ID"
  type        = string
  sensitive   = true
}

variable "nebius_account_id" {
  description = "Service account ID"
  type        = string
  sensitive   = true
}

# SSH access
variable "ssh_user_name" {
  description = "SSH username."
  type        = string
  default     = "ubuntu"
}

variable "region" {
  description = "Project region."
  type        = string
}

# Platform
variable "platform" {
  description = "Platform for S3 test host."
  type        = string
  default     = null
}

variable "preset" {
  description = "Preset for S3 test host."
  type        = string
  default     = null
}
