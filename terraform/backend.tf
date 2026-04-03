terraform {
  backend "s3" {
    bucket       = "supplychain360-terraform-state-bucket"
    key          = "prod/terraform.tfstate"
    region       = "eu-west-2"
    use_lockfile = true
  }
}