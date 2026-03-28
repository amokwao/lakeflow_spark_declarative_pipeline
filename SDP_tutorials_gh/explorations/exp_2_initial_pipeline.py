from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Staging layer 

@dp.table(
    name = 'bronze_orders',
    comment = 'staging table on source.orders',
    table_properties={
        'quality':'bronze'
    }
)
def bronze_orders():
https://dbc-b5b8ef90-5e08.cloud.databricks.com/browse/folders/164129292222102?contextId=pipeline%3A7102e993-83df-4691-abe9-1c1010b914d9&o=7474645257832006$0    df = spark.readStream.table('sdp_tutorial.source.orders')
    return df

# silver layer with mininal filter or cleaning 
@dp.temporary_view(
    name = 'silver_orders',
    comment = 'silver view on bronze_orders, has some transformation on status'
)
def silver_orders():
    df = spark.readStream.table('bronze_orders')
    df.withColumn('order_status', F.upper(F.col('order_status')))
    return df
# gold layee

@dp.table(
    name = 'gold_orders',
    comment = 'gold agg table on silver_orders.',
    table_properties={
        'quality':'gold'
    }
)
def bronze_orders():
    df = spark.readStream.table('silver_orders')
    df.groupBy('customer_id').agg(F.sum(F.col('order_value')).alias('customer_value'), F.count(F.col('order_id')).alias('total_sales'))
    return df