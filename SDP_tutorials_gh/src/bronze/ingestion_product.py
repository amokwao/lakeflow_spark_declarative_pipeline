
from pyspark import pipelines as dp
from pyspark.sql import functions as F
# https://docs.databricks.com/aws/en/ldp/expectation-patterns#quarantine-invalid-records
product_expectation_rules = {
    'valid product_id': "product_id IS NOT NULL",
    "valid product price": "product_price > 0"

}
quarantine_rules = "NOT({0})".format(" AND ".join(product_expectation_rules.values()))
# """NOT(product_id IS NOT NULL AND product_price > 0)"""


@dp.view(
    name = 'raw_products'
)
def raw_products():
    return spark.readStream.table('sdp_tutorial.source.products')


@dp.table(
    name = 'products_quarantine',
    comment = 'quarantine table for products'
)
@dp.expect_all(product_expectation_rules)
def products_quarantine():
    return (
        spark.readStream.table('raw_products')
        .withColumn("is_quarantined", F.expr(quarantine_rules))
    )

@dp.view
def valid_products():
  return spark.readStream.table("products_quarantine").filter("is_quarantined=false")

@dp.view
def invalid_products():
  return spark.readStream.table("products_quarantine").filter("is_quarantined=true")