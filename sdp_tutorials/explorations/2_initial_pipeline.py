from pyspark import pipelines as dp
import pyspark.sql.functions as func

# Staging layer

@dp.table(
    name='bronze_orders',
    comment='staging table of source.orders for SDP created by gak',
    table_properties={
        'quality': 'bronze'
    }
)
def bronze_orders(): # -> dp.Table:
    df = spark.readStream.table('sdp_tutorial.source.orders')
    return df


# silver layer with mininal filter or cleaning 
@dp.temporary_view(
    name = 'silver_orders',
    comment = 'silver view on bronze_orders, has some transformation on status',
    # table_properties={
    #     'quality': 'silver'
    # }
)
def silver_orders():
    df = spark.readStream.table('bronze_orders')
    df.withColumn('order_status', func.upper(func.col('order_status')))
    return df

# gold layer

@dp.table(
    name = 'gold_orders',
    comment = 'gold agg table on silver_orders.',
    table_properties={
        'quality':'gold'
    }
)
def bronze_orders():
    df = spark.readStream.table('silver_orders')
    df.groupBy('customer_id').agg(func.sum(func.col('order_value')).alias('customer_value'), func.count(func.col('order_id')).alias('total_sales'))
    return df