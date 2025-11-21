# utilities/quarantine.py
from pyspark.sql import DataFrame, SparkSession, functions as F

def add_raw_payload(df: DataFrame, col_name: str = "_raw_payload") -> DataFrame:
    return df.withColumn(col_name, F.to_json(F.struct(*[F.col(c) for c in df.columns])))

def build_quarantine_df(df: DataFrame,
                        source_table: str,
                        pk_value_col: str = None,
                        fk_values: str = None,
                        bad_column: str = None,
                        bad_value_col: str = None,
                        failure_reason: str = None,
                        raw_col: str = "_raw_payload") -> DataFrame:
    return df.select(
        F.lit(source_table).alias("source_table"),
        (F.col(pk_value_col).cast("string") if pk_value_col else F.lit(None)).alias("pk_value"),
        (F.col(fk_values).cast("string") if fk_values else F.lit(None)).alias("fk_values"),
        F.lit(bad_column).alias("bad_column"),
        (F.col(bad_value_col).cast("string") if bad_value_col else F.lit(None)).alias("bad_value"),
        F.lit(failure_reason).alias("failure_reason"),
        F.col(raw_col).alias("full_row")
    )

def append_quarantine(delta_target: str, quarantine_df: DataFrame, spark: SparkSession):
    quarantine_df.write.format("delta").mode("append").saveAsTable(delta_target)