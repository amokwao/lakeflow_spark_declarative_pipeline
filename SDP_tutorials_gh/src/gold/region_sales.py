from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name = 'business_sales',
    comment = 'sales of business with region',
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "quality":"Gold"
        }
)
def business_sales():

    df_fact_sales = spark.read.table('fact_sales')
    df_dim_product = spark.read.table('dim_product')
    df_dim_customer = spark.read.table('dim_customer')

    df_join  = (df_fact_sales
                .join(
                        df_dim_customer, df_fact_sales.customer_id == df_dim_customer.customer_id,'inner'
                    )
                .join(
                        df_dim_product, df_fact_sales.product_id == df_dim_product.product_id,'inner'
                    )
                )
    
    df_final = (df_join
                .withColumn('total_sales', F.col('sale_quantity') * F.col('sale_amount'))
                .select('region', "product_category", "total_sales")
                )
    
    df_agg = df_final.groupBy('region', "product_category").agg(F.sum('total_sales').alias('total_revenue'))
    
    return df_agg
    
