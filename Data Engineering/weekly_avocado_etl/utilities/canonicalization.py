# utilities/canonicalization.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List

def safe_trim(col: str):
    """Trim string and nullify empty."""
    return F.when(F.trim(F.col(col)) != "", F.trim(F.col(col))).otherwise(None)

def initcap_null(col: str):
    """Trim, nullify empty, then initcap."""
    return F.when(F.trim(F.col(col)) != "", F.initcap(F.trim(F.col(col)))).otherwise(None)

def canonicalize_string_columns(df: DataFrame, cols: List[str]) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, initcap_null(c))
    return df
