terraform {
  required_version = ">= 1.6.0"
}

variable "vault_addr" {
  default = "https://vault.internal.invalid"
}

provider "vault" {
  address = var.vault_addr
  token   = "hvs.k2j4h6g8f0d2s4a6p8o1i3u5y7t9r0e2"
}
