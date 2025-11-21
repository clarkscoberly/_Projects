# utilities/validation.py
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import List

def add_row_rank(df: DataFrame, partition_cols: List[str], order_col: str, out_col: str = "rnk") -> DataFrame:
    w = Window.partitionBy(*[F.col(c) for c in partition_cols]).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn(out_col, F.row_number().over(w))

def flag_null_pk(df: DataFrame, pk_col: str, flag_col: str = "_null_pk") -> DataFrame:
    return df.withColumn(flag_col, F.when(F.col(pk_col).isNull(), F.lit(1)).otherwise(F.lit(0)))

def date_sanity_flags(df: DataFrame, born_col: str, picked_col: str, sold_col: str) -> DataFrame:
    return (df
            .withColumn("invalid_picked_date", F.when(F.col(picked_col) < F.col(born_col), 1).otherwise(0))
            .withColumn("invalid_sold_date", F.when(F.col(sold_col) < F.col(picked_col), 1).otherwise(0))
           )