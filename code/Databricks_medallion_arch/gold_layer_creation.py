"""gold_dim_customers creation"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG de_pyspark_training_catalog")
spark.sql("USE SCHEMA buddy_group_4")

silver_customers = "makeup_exam_wojtyniakk_silver_customers"

scd2 = [
  "cust_address_country_id",
  "cust_address_state_province",
  "cust_address_city",
  "cust_address_postal_code",
  "cust_address_street_address",
  "marital_status"
]

w = Window.partitionBy("customer_id").orderBy("consume_datetime")

dim_customers = (
  spark.table(silver_customers)
    .withColumn(
      "is_new",
      (row_number().over(w) == 1)
      | reduce(
          lambda a, b: a | b,
          [lag(col(c), 1).over(w) != col(c) for c in scd2]
        )
    )
    .filter("is_new")
    .withColumn("effective_from", col("consume_datetime"))
    .withColumn("effective_to",   lead("consume_datetime").over(w))
    .withColumn("cust_sk", xxhash64(col("customer_id")))
    .withColumn("inserted_datetime", current_timestamp())
    .withColumn("updated_datetime",  current_timestamp())

    .select(
      col("cust_sk"),
      col("customer_id").alias("cust_nk"),
      col("cust_first_name"),
      col("cust_last_name"),
      *[col(c) for c in scd2],
      col("phone_number").alias("cust_phone_number"),
      col("cust_email"),
      col("account_mgr_id"),
      col("date_of_birth"),
      col("gender"),
      col("effective_from"),
      col("effective_to"),
      col("inserted_datetime"),
      col("updated_datetime")
    )
)

dim_customers.write \
  .mode("overwrite") \
  .format("delta") \
  .saveAsTable("makeup_exam_wojtyniakk_gold_dim_customers")
  
  
  
  """gold_dim_products creation"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG de_pyspark_training_catalog")
spark.sql("USE SCHEMA buddy_group_4")

silver_products = "makeup_exam_wojtyniakk_silver_products"

scd2_products = [
  "category_name",
  "product_status",
  "list_price"
]
 w_products = Window.partitionBy("product_id").orderBy("consume_datetime")
dim_products = (
  spark.table(silver_products)
    .withColumn(
      "is_new",
      (row_number().over(w_products) == 1)
      | reduce(
          lambda a, b: a | b,
          [lag(col(c), 1).over(w_products) != col(c) for c in scd2_products]
        )
    )
    .filter("is_new")
    
    .withColumn("effective_from", col("consume_datetime"))
    .withColumn("effective_to",   lead("consume_datetime").over(w_products))
    .withColumn("product_sk", xxhash64(col("product_id")))
    .withColumn("inserted_datetime", current_timestamp())
    .withColumn("updated_datetime",  current_timestamp())
    
    .select(
      col("product_sk"),
      col("product_id").alias("product_nk"),
      col("product_name"),
      *[col(c) for c in scd2_products],
      col("min_price"),
      col("weight_class"),
      col("effective_from"),
      col("effective_to"),
      col("inserted_datetime"),
      col("updated_datetime")
    )
)

dim_products.write \
  .mode("overwrite") \
  .format("delta") \
  .saveAsTable("makeup_exam_wojtyniakk_gold_dim_products")

print(f" Gold dim table created: {dim_products}")


"""gold_fact_table creation"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, xxhash64, current_timestamp, col
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG de_pyspark_training_catalog")
spark.sql("USE SCHEMA buddy_group_4")

orders = spark.table("makeup_exam_wojtyniakk_silver_orders") \
    .select("order_id", "customer_id", "order_date", "order_mode", "order_status")

items = spark.table("makeup_exam_wojtyniakk_silver_order_items") \
    .select("order_id", "line_item_id", "product_id", "unit_price", "quantity", "consume_datetime")

# Join orders and items to form fact base
df = orders.join(items, "order_id")

# Read gold dimensions
cust_dim = spark.table("makeup_exam_wojtyniakk_gold_dim_customers") \
    .select("cust_sk", "cust_nk", "effective_from", "effective_to")

prod_dim = spark.table("makeup_exam_wojtyniakk_gold_dim_products") \
    .select("product_sk", "product_nk", "effective_from", "effective_to")

# Join to get surrogate keys with time validity
df = df.join(
    cust_dim,
    (df.customer_id == cust_dim.cust_nk) &
    (df.consume_datetime >= cust_dim.effective_from) &
    ((cust_dim.effective_to.isNull()) | (df.consume_datetime < cust_dim.effective_to)),
    "left"
)

df = df.join(
    prod_dim,
    (df.product_id == prod_dim.product_nk) &
    (df.consume_datetime >= prod_dim.effective_from) &
    ((prod_dim.effective_to.isNull()) | (df.consume_datetime < prod_dim.effective_to)),
    "left"
)

# Build business NK and surrogate SK
df = df.withColumn("order_nk", concat_ws("|",
        col("order_id"), col("line_item_id"), col("customer_id"), col("product_id")
    )) \
    .withColumn("order_sk", xxhash64(col("order_nk"))) \
    .withColumn("inserted_datetime", current_timestamp())

# Select final fact structure and write
(df.select(
    "order_sk",
    col("cust_sk").alias("customer_sk"),
    col("product_sk").alias("product_sk"),
    "order_nk",
    col("customer_id").alias("customer_nk"),
    col("product_id").alias("product_nk"),
    "order_id",
    "line_item_id",
    "order_date",
    "order_mode",
    "order_status",
    "unit_price",
    "quantity",
    "inserted_datetime"
)
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable("makeup_exam_wojtyniakk_gold_fact_orders"))

print("Gold fact orders table created successfully")
