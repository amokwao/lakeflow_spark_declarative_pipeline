
from pyspark import pipelines as dp
from pyspark.sql import functions as F
# Create_sauto_cdc_flow
# Databricks Documents
# https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes

# Step1: Create a empty streaming table 
dp.create_streaming_table("dim_product")

dp.create_auto_cdc_flow(
  target = "dim_product",
  source = "valid_products",
  keys = ["product_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)