import sys
from typing import List, Union, Optional
from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf
from pyspark.sql.types import (
    StructType, IntegerType, StringType, FloatType, DoubleType, ArrayType, MapType,
    BooleanType, ByteType, ShortType, LongType, TimestampType, DateType, BinaryType
)

spark = SparkSession.builder.appName("validationUtils").getOrCreate()

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

        print(f"selecting {num_rows} rows for {name}")

        if num_rows and num_rows > 0:
            table_name = temp_dev_name(name)
            if table_name == -1:
                return -1

            columns = ", ".join(columns_to_select)
            s = f"CREATE OR REPLACE TABLE {table_name} AS SELECT {columns} FROM {name} TABLESAMPLE ({num_rows} ROWS)"
            print(s.replace("SELECT", "\nSELECT").replace("FROM", "\nFROM").replace("TABLESAMPLE", "\nTABLESAMPLE"))

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


# primitive_types = [
#     IntegerType(),
#     FloatType(),
#     DoubleType(),
#     StringType(),
#     BooleanType(),
#     ByteType(),
#     ShortType(),
#     LongType(),
#     TimestampType(),
#     DateType(),
#     BinaryType()
# ]
# array_of_primitive_types = [ArrayType(x) for x in primitive_types]
# map_of_primitive_types = [MapType(x,y) for x in primitive_types for y in primitive_types]
# print(map_of_primitive_types)
# def parseSchema(schema: StructType,spaces = "") :
#     for sf in schema:
#         if sf.dataType in primitive_types :
#             print(spaces,"primitive",sf.name,sf.dataType)
#         if sf.dataType in array_of_primitive_types :
#             print(spaces,"array of primitive",sf.name,sf.dataType)
#         elif sf.dataType in map_of_primitive_types :
#             print(spaces,"primitive",sf.name,sf.dataType)


def compareDFs(df1 : DataFrame, df2: DataFrame) :
    df1_schema_map = {x.name : x.dataType for x in df1.schema}
    df2_schema_map = {x.name : x.dataType for x in df2.schema}
    print(df1_schema_map)
    print(df1_schema_map)
    shared_schemas = []
    for name, dtype in df1_schema_map.items():
        if name in df2_schema_map and df2_schema_map[name] == dtype:
            shared_schemas.append(name)

    print(shared_schemas)






# compare PK when its comparable, get PK for gold base tables with sampling
# compare hash of table with sampling
#compare PK without sampling
# compare hash of table without sampling
