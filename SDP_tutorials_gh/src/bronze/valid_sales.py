from pyspark import pipelines as dp
from pyspark.sql import functions as F
# https://docs.databricks.com/aws/en/ldp/expectation-patterns#quarantine-invalid-records

sales_expectation_rules = {
    'valid sales sales_id': "sales_id IS NOT NULL",
    "valid sales customer_id": "customer_id IS NOT NULL",
    "valid sales product_id": "product_id IS NOT NULL",
    "valid sales quantity": "sale_quantity > 0"

}

quarantine_rules = "NOT({0})".format(" AND ".join(sales_expectation_rules.values()))
# """NOT(product_id IS NOT NULL AND product_price > 0)"""


@dp.view(
    name = 'raw_sales'
)
def raw_sales():
    return spark.readStream.table('bronze_region_sales')


@dp.table(
    name = 'sales_quarantine',
    comment = 'quarantine table for sales'
)
@dp.expect_all(sales_expectation_rules)
def sales_quarantine():
    return (
        spark.readStream.table('raw_sales')
        .withColumn("is_quarantined", F.expr(quarantine_rules))
    )

@dp.view
def valid_sales():
  return spark.readStream.table("sales_quarantine").filter("is_quarantined=false")

@dp.view
def invalid_sales():
  return spark.readStream.table("sales_quarantine").filter("is_quarantined=true")