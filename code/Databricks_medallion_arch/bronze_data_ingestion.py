from pyspark.sql.functions import current_timestamp, lit

username        = "wojtyniakk"
input_path      = "s3://de-40-training-raw/final_exam_data"
checkpoint_path = f"s3://de-40-training-raw/output_data/buddy_group_4/makeup_exam_{username}_checkpoint"
bronze_path     = f"s3://de-40-training-raw/output_data/buddy_group_4/makeup_exam_{username}_bronze"

paths = {
    "customers":   f"{input_path}/customers/",
    "orders":      f"{input_path}/orders/",
    "order_items": f"{input_path}/order_items/",
    "products":    f"{input_path}/products/"
}

checkpoint_paths = {key: f"{checkpoint_path}/{key}/" for key in paths}
bronze_paths     = {key: f"{bronze_path}/{key}/"     for key in paths}

def ingest_to_bronze(entity, table_name=None):
    src        = paths[entity]
    chkpt      = checkpoint_paths[entity]
    tgt_path   = bronze_paths[entity]
    hive_table = table_name or f"makeup_exam_{username}_bronze_{entity}"

    query = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", True)
             .option("delimiter", ",")
             .option("inferSchema", True)
             .option("cloudFiles.schemaLocation", chkpt + "schema/")
             .option("cloudFiles.includeExistingFiles", True)
             .load(src)
        .writeStream
             .foreachBatch(lambda df, batch_id: (
                 df
                   .withColumn("ingest_datetime", current_timestamp())
                   .withColumn("batch_id", lit(batch_id))
                   .write
                     .format("delta")
                     .mode("append")
                     .option("mergeSchema", "true")
                     .option("path", tgt_path)
                     .saveAsTable(hive_table)
             ))
             .option("checkpointLocation", chkpt)
             .trigger(once=True)
             .start()
    )
    print(f"Started ingest for {entity}: table={hive_table}, path={tgt_path}")
    return query


queries = {tbl: ingest_to_bronze(tbl) for tbl in paths}

"""loop for loading tables from delta location"""

entities = ["customers", "orders", "order_items", "products"]
for ent in entities:
    tbl      = f"makeup_exam_{username}_bronze_{ent}"
    default_tbl = f"default.{tbl}"
    uc_tbl   = f"de_pyspark_training_catalog.buddy_group_4.{tbl}"
    loc      = bronze_paths[ent]

    spark.sql(f"DROP TABLE IF EXISTS {default_tbl}")
    print(f"Dropped default table if existed: {default_tbl}")

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {uc_tbl} \
         USING DELTA LOCATION '{loc}'"
    )
    print(f"Created UC table {uc_tbl} -> {loc}")

"""df check"""

username = "wojtyniakk"
entities = ["customers", "orders", "order_items", "products"]
for ent in entities:
    tbl_uc = f"de_pyspark_training_catalog.buddy_group_4.makeup_exam_{username}_bronze_{ent}"
    df = spark.table(tbl_uc)
    count = df.count()
    print(f"Table {tbl_uc} record count: {count}")
    df.show(20)