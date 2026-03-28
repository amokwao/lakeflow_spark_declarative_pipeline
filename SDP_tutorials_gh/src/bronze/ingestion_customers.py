from pyspark import pipelines as dp
from pyspark.sql import functions as F
# https://docs.databricks.com/aws/en/ldp/expectation-patterns#quarantine-invalid-records

customer_expectation_rules = {
    'valid customer id': "customer_id IS NOT NULL",
    "valid customer name": "customer_name IS NOT NULL",
    "Valid customer region": "region IS NOT NULL"

}
quarantine_rules = "NOT({0})".format(" AND ".join(customer_expectation_rules.values()))
# """NOT(customer_id IS NOT NULL AND customer_name IS NOT NULL AND region IS NOT NULL)"""


@dp.view(
    name = 'raw_customers'
)
def raw_customers():
    return spark.readStream.table('sdp_tutorial.source.customers')


@dp.table(
    name = 'customers_quarantine',
    comment = 'quarantine table for customer'
)
@dp.expect_all(customer_expectation_rules)
def customers_quarantine():
    return (
        spark.readStream.table('raw_customers')
        .withColumn("is_quarantined", F.expr(quarantine_rules))
    )

@dp.view
def valid_customer():
  return spark.readStream.table("customers_quarantine").filter("is_quarantined=false")

@dp.view
def invalid_customer():
  return spark.readStream.table("customers_quarantine").filter("is_quarantined=true")