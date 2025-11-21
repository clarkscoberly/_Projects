# utilities/metadata.py
from pyspark.sql import DataFrame, functions as F
import uuid

def add_ingest_metadata(df, etl_pipeline, etl_version):
    df = df.withColumn("_ingest_ts", F.current_timestamp())
    df = df.withColumn("_source_file", F.col("_metadata.file_path"))
    df = df.withColumn("_etl_pipeline", F.lit(etl_pipeline))
    df = df.withColumn("_etl_version", F.lit(etl_version))
    return df
    

def add_validation_metadata(df: DataFrame, status_col: str = "_validation_status",
                            reason_col: str = "_validation_reason",
                            status: str = "valid", reason: str = None) -> DataFrame:
    """
    Add validation metadata for traceability.
    """
    df = df.withColumn(status_col, F.lit(status))
    if reason:
        df = df.withColumn(reason_col, F.lit(reason))
    else:
        df = df.withColumn(reason_col, F.lit(None))
    return df
