from pyspark import pipelines as dp

@dp.materialized_view(
    name='orders_mv',
    comment='MV on source.ordres, for SDP purpose'
)
def orders_mv():
    df = spark.read.table('sdp_tutorial.source.orders')
    return df


@dp.table()
def orders_steam():
    df = spark.readStream.table('sdp_tutorial.source.orders')
    return df


@dp.temporary_view(
    name = 'orders_vw'
)
def orders():
    df = spark.read.table('sdp_tutorial.source.orders')
    return df