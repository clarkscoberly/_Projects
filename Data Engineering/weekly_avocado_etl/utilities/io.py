# utilities/io.py
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from typing import List

def write_table_overwrite(df: DataFrame, table_name: str, partition_cols: List[str] = None):
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_name)

def merge_into_delta(spark: SparkSession, target_table: str, source_df: DataFrame, merge_keys: List[str]):
    join_cond = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    if not spark._jsparkSession.catalog().tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return
    delta_table = DeltaTable.forName(spark, target_table)
    (delta_table.alias("target")
               .merge(source_df.alias("source"), join_cond)
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())