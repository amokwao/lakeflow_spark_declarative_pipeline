from pyspark import pipelines as dp
from pyspark.sql import functions as F
# Databricks Documents
# https://docs.databricks.com/aws/en/ldp/flow-examples


# Create a empty streaming table 

# dp.create_streaming_table("bronze_region_sales", expect_all=sales_expectation_rules)
dp.create_streaming_table("bronze_region_sales")

# create a append flow, that loads the data into the target
@dp.append_flow(target = 'bronze_region_sales')
def sales_west():
    df = spark.readStream.table('sdp_tutorial.source.sales_west')

    return (
        df.select(
            'sales_id',
            'customer_id',
            'product_id',
            'sale_quantity',
            'sale_timestamp',
            'sale_amount'
        )
    )
    

@dp.append_flow(target = 'bronze_region_sales')
def sales_east():
    df = spark.readStream.table('sdp_tutorial.source.sales_east')
    
    return (
        df.select(
            'sales_id',
            'customer_id',
            'product_id',
            'sale_quantity',
            'sale_timestamp',
            'sale_amount'
        )
    )
    