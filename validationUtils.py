import sys
from typing import List, Union, Optional
from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf, date_trunc
from pyspark.sql.types import *

spark = SparkSession.builder.appName("validationUtils").getOrCreate()


def ts(df):
    df.show(20, False)
    return df


def temp_dev_name(name: str) -> Union[str, int]:
    """
    Generates the temp dev name for a edw table coursera_warehouse.<schema>.<name> -> dev.general.<schema>_<name>_cw
    Args:
        name (str): The fully qualified table name (catalog.schema.table).
    Returns:
        Union[str, int]: The generated temporary table name or -1 if invalid format.
    """
    try:
        parts = name.split(".")
        if len(parts) != 3:
            print(f"Error: Invalid table name format '{name}'. Expected 3 parts.")
            return -1
        catalog, schema, table = parts
        return f"dev.general.{schema}_{table}_cw"
    except Exception as e:
        print(f"Error in temp_dev_name: {e}")
        return -1


def create_sample_df(name: str, columns_to_select: List[str], size_in_mb: int) -> Union[str, int]:
    """
    Needed because a direct EDW load takes a lot of time, we create the table first.
    Calculates row count based on size and creates a sample table.
    Args:
        name (str): The source table name.
        columns_to_select (List[str]): List of columns to select.
        size_in_mb (int): Target size in MB.
    Returns:
        Union[str, int]: The name of the created table or -1 on failure.
    """
    try:
        sample_df_name = temp_dev_name(name)
        if sample_df_name == -1:
            return -1

        try:
            df = spark.read.table(name).select(*columns_to_select)
            sample_df = df.limit(100)
            rows = sample_df.collect()
        except Exception as e:
            print(f"Error reading sample table {sample_df_name}: {e}")
            return -1
        if not rows:
            print(f"No rows returned for {sample_df_name}")
            return -1

        avg_row_size = sum(sys.getsizeof(row.asDict()) for row in rows) / len(rows)
        target_bytes = size_in_mb * 1024 * 1024
        num_rows = int(target_bytes / avg_row_size) if avg_row_size > 0 else None

        if num_rows and num_rows > 0:
            table_name = temp_dev_name(name)
            if table_name == -1:
                return -1

            columns = ", ".join(columns_to_select)
            s = f"CREATE OR REPLACE TABLE {table_name} AS SELECT {columns} FROM {name} TABLESAMPLE ({num_rows} ROWS)"
            # print(s.replace("SELECT", "\nSELECT").replace("FROM", "\nFROM").replace("TABLESAMPLE", "\nTABLESAMPLE"))

            try:
                spark.sql(s)
                return str(table_name)
            except Exception as e:
                print(f"Error executing SQL: {e}")
                return -1

        return -1
    except Exception as e:
        print(f"Unexpected error in create_sample_df: {e}")
        return -1

def df_shared_schema(df1: DataFrame, df2: DataFrame):
    """
    returns shared schema column names for df1 and df2
    """
    df1_schema_map = {x.name: x.dataType for x in df1.schema}
    df2_schema_map = {x.name: x.dataType for x in df2.schema}
    shared_schemas = {}
    for name, dtype in df1_schema_map.items():
        if name in df2_schema_map and df2_schema_map[name] == dtype:
            shared_schemas[name] = dtype
    return shared_schemas


def hash_df(df: DataFrame, columns_to_hash: List[str], columns_to_keep: List[str]):
    for colName in columns_to_hash:
        df = df.withColumn(colName, col(colName).cast(StringType()))
    ans = df.withColumn("hash", f.hash(*columns_to_hash)).select(*(columns_to_keep + ["hash"]))
    return ans

def compare_schemas(df1: DataFrame, df2: DataFrame):
    df1_schema_map = {x.name: x.dataType for x in df1.schema}
    df2_schema_map = {x.name: x.dataType for x in df2.schema}

    df1_keys = set(df1_schema_map.keys())
    df2_keys = set(df2_schema_map.keys())
    common_keys = df1_keys.intersection(df2_keys)

    return {
        "df1_only": {k: df1_schema_map[k] for k in df1_keys - df2_keys},
        "df2_only": {k: df2_schema_map[k] for k in df2_keys - df1_keys},
        "same_types": {k: df1_schema_map[k] for k in common_keys if df1_schema_map[k] == df2_schema_map[k]},
        "diff_types": {k: (df1_schema_map[k], df2_schema_map[k]) for k in common_keys if df1_schema_map[k] != df2_schema_map[k]}
    }

def compareDFs(df1: DataFrame, df2: DataFrame, pk_cols, columns_to_compare: List[str] = None,sample_df1 : bool = False):
    shared_schema_columns = df_shared_schema(df1, df2).keys()
    if not columns_to_compare:
        columns_to_compare = shared_schema_columns
    for col in columns_to_compare:
        if col not in shared_schema_columns:
            raise ValueError(f"column_to_compare {col} is not present in shared_schema")
    for col in pk_cols:
        if col not in shared_schema_columns:
            raise ValueError(f"pk_column {col} is not present in shared_schema")
    if sample_df1:
        df1 = create_sample_df(df1, shared_schema_columns)
    hash_columns = [x for x in shared_schema_columns if x not in pk_cols]
    df1_truncated = hash_df(df1.select(*columns_to_compare),hash_columns,pk_cols)
    df2_truncated = hash_df(df2.select(*columns_to_compare),hash_columns,pk_cols)
    # comparing what fraction of rows of df1 are present in df2
    match_df = df1_truncated.join(df2_truncated, on=pk_cols, how="inner")\
    .withColumn("hash_match",df1_truncated["hash"] == df2_truncated["hash"]).filter(f.col("hash_match"))

    return match_df.count()/df1_truncated.count()



