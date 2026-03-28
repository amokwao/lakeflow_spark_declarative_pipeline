from pyspark import pipelines as dp

@dp.materialized_view(
    name='orders_mv',
    comment='materialized view of source.orders for SDP created by gak'
)
def orders_mv():
    df = spark.read.table('sdp_tutorial.source.orders')
    return df

@dp.table(
    name='orders_stream',
    comment='table of source.orders for SDP created by gak'
)
def orders_stream():
    df = spark.readStream.table('sdp_tutorial.source.orders')
    return df

@dp.view(
    name='orders_view',
    comment='view of source.orders for SDP created by gak'
)
def orders_view():
    df = spark.read.table('sdp_tutorial.source.orders')
    return df
