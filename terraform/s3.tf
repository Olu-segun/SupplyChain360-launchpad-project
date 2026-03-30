resource "aws_s3_bucket" "supplychain360-data-lake" {
  bucket = var.bucket_name

  tags = {
    Name = "supplychain360-bucket"
  }
}

resource "aws_s3_object" "supplychain360_raw_folders" {
  for_each = toset([
    "raw/product_catalog_master/",
    "raw/retail_store_locations/",
    "raw/supplier_registry_data/",
    "raw/warehouse_master_data/",
    "raw/warehouse_inventory/",
    "raw/shipment_delivery_logs/",
    "raw/store_sales_transactions/",
  ])

  bucket = aws_s3_bucket.supplychain360-data-lake.id
  key    = each.value
}
