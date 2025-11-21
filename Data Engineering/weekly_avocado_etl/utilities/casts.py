# utilities/casts.py
from pyspark.sql import DataFrame, functions as F

def try_cast_trim(col_name: str, target_type: str):
    """Mimics try_cast(nullif(trim(col), '') as target_type)"""
    return f"try_cast(nullif(trim({col_name}), '') as {target_type})"

def parse_int_col(df: DataFrame, col_name: str, out_col: str = None) -> DataFrame:
    out_col = out_col or col_name
    return df.withColumn(out_col, F.expr(try_cast_trim(col_name, "int")))

def parse_long_col(df: DataFrame, col_name: str, out_col: str = None) -> DataFrame:
    out_col = out_col or col_name
    return df.withColumn(out_col, F.expr(try_cast_trim(col_name, "bigint")))

def parse_double_col(df: DataFrame, col_name: str, out_col: str = None) -> DataFrame:
    out_col = out_col or col_name
    return df.withColumn(out_col, F.expr(try_cast_trim(col_name, "double")))

def parse_date_col(df: DataFrame, col_name: str, out_col: str = None, fmt: str = None) -> DataFrame:
    """Parse string -> date, optionally with format."""
    out_col = out_col or col_name
    if fmt:
        return df.withColumn(out_col, F.to_date(F.nullif(F.trim(F.col(col_name)), ""), fmt))
    return df.withColumn(out_col, F.expr(try_cast_trim(col_name, "date")))