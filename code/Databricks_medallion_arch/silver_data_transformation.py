"""silver_customers_transformation"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start Spark and set schema
spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG de_pyspark_training_catalog")
spark.sql("USE SCHEMA buddy_group_4")

# table names for customers only
bronze = "de_pyspark_training_catalog.buddy_group_4.makeup_exam_wojtyniakk_bronze_customers"
silver = "de_pyspark_training_catalog.buddy_group_4.makeup_exam_wojtyniakk_silver_customers"

# read bronze
df = spark.read.table(bronze)

# transform lowercase and replace dots
cols = [c.lower().replace('.', '_') for c in df.columns]
df = df.toDF(*cols)

# drop batch_id if it's there
df = df.drop(*['batch_id']) if 'batch_id' in df.columns else df

# rename ingest_datetime
if 'ingest_datetime' in df.columns:
    df = df.withColumnRenamed('ingest_datetime','consume_datetime')

# parse dates and times
for c in df.columns:
    if c == 'date_of_birth':
        dob = to_date(df[c], 'dd-MMM-yy')
        df = df.withColumn('date_of_birth', when(dob > current_date(), add_months(dob, -1200)).otherwise(dob))
    elif 'date' in c:
        df = df.withColumn(c, to_date(df[c]))
    elif 'datetime' in c or 'timestamp' in c:
        df = df.withColumn(c, to_timestamp(df[c]))

# phone format
if 'phone_number' in df.columns:
    df = df.withColumn('phone_number', regexp_replace(df['phone_number'], r"(\d{3})(\d{3})(\d{4})", r"$1-$2-$3"))

# add inserted timestamp
df = df.withColumn('inserted_datetime', current_timestamp())

# dedupe using all other columns
keep = [c for c in df.columns if c not in ('consume_datetime','inserted_datetime')]
w = Window.partitionBy(*keep).orderBy('consume_datetime')
df = df.withColumn('rn', row_number().over(w)).filter('rn=1').drop('rn')

# write silver
(df.write.format('delta').mode('overwrite').saveAsTable(silver))
print('Silver customers done')


"""silver_orders/order_items/products_transformation"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# start Spark and set schema
spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG de_pyspark_training_catalog")
spark.sql("USE SCHEMA buddy_group_4")

# table names for orders only
bronze = "de_pyspark_training_catalog.buddy_group_4.makeup_exam_wojtyniakk_bronze_orders"
silver = "de_pyspark_training_catalog.buddy_group_4.makeup_exam_wojtyniakk_silver_orders"

# read bronze
df = spark.read.table(bronze)

# simple cleanup: lowercase and replace dots
cols = [c.lower().replace('.', '_') for c in df.columns]
df = df.toDF(*cols)

# drop batch_id
df = df.drop('batch_id') if 'batch_id' in df.columns else df

# rename ingest_datetime
if 'ingest_datetime' in df.columns:
    df = df.withColumnRenamed('ingest_datetime', 'consume_datetime')

# parse order_date with pattern matching to avoid errors
if 'order_date' in df.columns:
    df = df.withColumn(
        'order_date',
        when(
            col('order_date').rlike(r"^[0-9]{2}-[A-Z]{3}-[0-9]{2} [0-9]{2}\.[0-9]{2}\.[0-9]{2}\.[0-9]{6} [AP]M$"),
            to_timestamp(col('order_date'), 'dd-MMM-yy hh.mm.ss.SSSSSS a')
        )
        .when(
            col('order_date').rlike(r"^[0-9]{2}-[A-Z]{3}-[0-9]{2}$"),
            to_date(col('order_date'), 'dd-MMM-yy').cast('timestamp')
        )
        .otherwise(lit(None).cast('timestamp'))
    )

# add inserted timestamp
df = df.withColumn('inserted_datetime', current_timestamp())

# dedupe using all other columns
keep = [c for c in df.columns if c not in ('consume_datetime', 'inserted_datetime')]
w = Window.partitionBy(*keep).orderBy('consume_datetime')
df = df.withColumn('rn', row_number().over(w)).filter('rn=1').drop('rn')

# write silver
df.write.format('delta').mode('overwrite').saveAsTable(silver)
print('Silver orders done')
