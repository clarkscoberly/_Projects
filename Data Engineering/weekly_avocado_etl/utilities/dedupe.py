# utilities/dedupe.py

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import List

def dedupe_keep_latest(df: DataFrame, partition_cols: List[str], order_col: str, keep_cols: List[str] = None) -> DataFrame:
    w = Window.partitionBy(*[F.col(c) for c in partition_cols]).orderBy(F.col(order_col).desc_nulls_last())
    ranked = df.withColumn("_rnk", F.row_number().over(w))
    df_out = ranked.filter(F.col("_rnk") == 1).drop("_rnk")
    if keep_cols:
        return df_out.select(*keep_cols)
    return df_out