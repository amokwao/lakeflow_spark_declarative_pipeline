from pyspark import pipelines as dp
# Sink
# https://docs.databricks.com/aws/en/ldp/ldp-sinks


dp.create_sink(
    name = 'region_business_sales',
    format = 'delta',
    options={"tableName": "workspace.default.region_business_sales"}
)

@dp.append_flow(target = 'region_business_sales')
def region_business_sales():
  return spark.readStream.table('sdp_tutorial.sales_dbw.business_sales')


@dp.materialized_view(
  name = 'users_python'
)
def users_python():
  return spark.read.table('users')