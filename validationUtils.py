import sys
import logging
from typing import List, Union, Optional, Dict
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import *


class ValidationUtils:
    def __init__(self, spark: SparkSession, enable_logging: bool = True):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Configure logging if not already configured
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.logger.setLevel(logging.INFO if enable_logging else logging.CRITICAL)

    def log(self, message: str, level: str = "info"):
        """Wrapper for logging to simplify calls."""
        if level.lower() == "info":
            self.logger.info(message)
        elif level.lower() == "error":
            self.logger.error(message)
        elif level.lower() == "warning":
            self.logger.warning(message)
        elif level.lower() == "debug":
            self.logger.debug(message)

    def ts(self, df: DataFrame) -> DataFrame:
        df.show(20, False)
        return df

    def temp_dev_name(self, name: str) -> Union[str, int]:
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
                self.log(f"Error: Invalid table name format '{name}'. Expected 3 parts.", "error")
                return -1
            catalog, schema, table = parts
            return f"dev.general.{schema}_{table}_cw"
        except Exception as e:
            self.log(f"Error in temp_dev_name: {e}", "error")
            return -1

    def create_sample_df(self, name: str, columns_to_select: List[str], size_in_mb: int) -> Union[str, int]:
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
            sample_df_name = self.temp_dev_name(name)
            if sample_df_name == -1:
                return -1

            try:
                df = self.spark.read.table(name).select(*columns_to_select)
                sample_df = df.limit(100)
                rows = sample_df.collect()
            except Exception as e:
                self.log(f"Error reading sample table {sample_df_name}: {e}", "error")
                return -1
            if not rows:
                self.log(f"No rows returned for {sample_df_name}", "error")
                return -1

            avg_row_size = sum(sys.getsizeof(row.asDict()) for row in rows) / len(rows)
            target_bytes = size_in_mb * 1024 * 1024
            num_rows = int(target_bytes / avg_row_size) if avg_row_size > 0 else None

            if num_rows and num_rows > 0:
                table_name = self.temp_dev_name(name)
                if table_name == -1:
                    return -1

                columns = ", ".join(columns_to_select)
                s = f"CREATE OR REPLACE TABLE {table_name} AS SELECT {columns} FROM {name} TABLESAMPLE ({num_rows} ROWS)"

                try:
                    self.spark.sql(s)
                    return str(table_name)
                except Exception as e:
                    self.log(f"Error executing SQL: {e}", "error")
                    return -1

            return -1
        except Exception as e:
            self.log(f"Unexpected error in create_sample_df: {e}", "error")
            return -1

    def df_shared_schema(self, df1: DataFrame, df2: DataFrame) -> Dict[str, DataType]:
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

    def hash_df(self, df: DataFrame, columns_to_hash: List[str], columns_to_keep: List[str]) -> DataFrame:
        self.log(str(columns_to_hash) + "." + str(columns_to_keep))
        if not columns_to_hash:
            ans = df.withColumn("hash", f.lit(1)).select(*(columns_to_keep + ["hash"]))
        else:
            for colName in columns_to_hash:
                df = df.withColumn(colName, col(colName).cast(StringType()))
            ans = df.withColumn("hash", f.hash(*columns_to_hash)).select(*(columns_to_keep + ["hash"]))
        return ans

    def compare_schemas(self, df1: DataFrame, df2: DataFrame) -> Dict:
        df1_schema_map = {x.name: x.dataType for x in df1.schema}
        df2_schema_map = {x.name: x.dataType for x in df2.schema}

        df1_keys = set(df1_schema_map.keys())
        df2_keys = set(df2_schema_map.keys())
        common_keys = df1_keys.intersection(df2_keys)

        return {
            "df1_only": {k: df1_schema_map[k] for k in df1_keys - df2_keys},
            "df2_only": {k: df2_schema_map[k] for k in df2_keys - df1_keys},
            "same_types": {k: df1_schema_map[k] for k in common_keys if df1_schema_map[k] == df2_schema_map[k]},
            "diff_types": {k: (df1_schema_map[k], df2_schema_map[k]) for k in common_keys if
                           df1_schema_map[k] != df2_schema_map[k]}
        }

    def compareDFs(self, df1: DataFrame, df2: DataFrame, pk_cols: List[str],
                   columns_to_compare: List[str] = None) -> float:
        shared_schema_columns = list(self.df_shared_schema(df1, df2).keys())
        self.log(shared_schema_columns)
        if not columns_to_compare:
            columns_to_compare = shared_schema_columns
        for col in columns_to_compare:
            if col not in shared_schema_columns:
                raise ValueError(f"column_to_compare {col} is not present in shared_schema")
        for col in pk_cols:
            if col not in shared_schema_columns:
                raise ValueError(f"pk_column {col} is not present in shared_schema")

        hash_columns = [x for x in shared_schema_columns if x not in pk_cols]
        df1_truncated = self.hash_df(df1.select(*columns_to_compare), hash_columns, pk_cols)
        df2_truncated = self.hash_df(df2.select(*columns_to_compare), hash_columns, pk_cols)

        # comparing what fraction of rows of df1 are present in df2
        match_df = df1_truncated.join(df2_truncated, on=(pk_cols + ["hash"]), how="inner")

        count_df1 = df1_truncated.count()
        if count_df1 == 0:
            return 0.0

        return match_df.count() / count_df1
