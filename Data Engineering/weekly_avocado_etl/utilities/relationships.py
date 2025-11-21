# utilities/relationships.py
from pyspark.sql import DataFrame, functions as F

def find_multi_fk_violations(df: DataFrame, group_by_col: str, fk_col: str) -> DataFrame:
    return (df.groupBy(group_by_col)
              .agg(F.collect_set(fk_col).alias("fk_set"))
              .filter(F.size(F.col("fk_set")) > 1)
           )
