import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType
from validationUtils import *


class ValidationUtilsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ValidationUtilsTest") \
            .master("local[2]") \
            .getOrCreate()
        # Common fields schema
        # common_1: Array of Strings
        # common_2: Struct { sub_a: int, sub_b: string }
        # common_3: Map { string -> int }

        common_2_struct = StructType([
            StructField("sub_a", IntegerType(), True),
            StructField("sub_b", StringType(), True)
        ])

        # EDW Schema: pk_1, pk_2, common_1, common_2, common_3, edw_only
        edw_schema = StructType([
            StructField("pk_1", IntegerType(), True),
            StructField("pk_2", StringType(), True),
            StructField("common_1", ArrayType(StringType()), True),
            StructField("common_2", common_2_struct, True),
            StructField("common_3", MapType(StringType(), IntegerType()), True),
            StructField("common_4", IntegerType(), True),
            StructField("edw_only", StringType(), True)
        ])

        # EDS Schema: pk_1, pk_2, common_1, common_2, common_3, eds_only
        eds_schema = StructType([
            StructField("pk_1", IntegerType(), True),
            StructField("pk_2", StringType(), True),
            StructField("common_1", ArrayType(StringType()), True),
            StructField("common_2", common_2_struct, True),
            StructField("common_3", MapType(StringType(), IntegerType()), True),
            StructField("common_4", StringType(), True),
            StructField("eds_only", StringType(), True)
        ])

        # Data for EDW (pk 1, 2, 3, 4, 5)
        # common_1: ["a", "b"]
        # common_2: (100, "foo")
        # common_3: {"k1": 1, "k2": 2}
        edw_data = [
            (1, "A", ["val_1a", "x"], (10, "sub1"), {"k1": 1}, 1, "edw_val_1"),
            (2, "B", ["val_2b", "y"], (20, "sub2"), {"k2": 2}, 2, "edw_val_2"),
            (3, "C", ["val_3c", "z"], (30, "sub3"), {"k3": 3}, 3, "edw_val_3"),
            (4, "D", ["val_4d", "w"], (40, "sub4"), {"k4": 4}, 4, "edw_val_4"),
            (5, "E", ["val_5e", "v"], (50, "sub5"), {"k5": 5}, 5, "edw_val_5")
        ]

        # Data for EDS (pk 2, 3, 4, 5, 6)
        eds_data = [
            (2, "B", ["val_2b", "y"], (20, "sub2"), {"k2": 4}, "1", "eds_val_2"),
            (3, "C", ["val_3c", "z"], (30, "sub3"), {"k3": 3}, "2", "eds_val_3"),
            (4, "D", ["val_4d", "w"], (40, "sub4"), {"k4": 4}, "3", "eds_val_4"),
            (5, "E", ["val_5e", "v"], (50, "sub5"), {"k5": 5}, "4", "eds_val_5"),
            (6, "F", ["val_6f", "u"], (60, "sub6"), {"k6": 6}, "5", "eds_val_6")
        ]

        cls.df_edw = cls.spark.createDataFrame(edw_data, schema=edw_schema)
        cls.df_eds = cls.spark.createDataFrame(eds_data, schema=eds_schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_dataframes_creation(self):
        df_shared_schema(self.df_edw, self.df_eds)

    def test_df_hash(self) :
        hash_df(self.df_edw,["common_1","common_2","common_3"],["pk_1"])
        hash_df(self.df_eds, ["common_1", "common_2", "common_3"], ["pk_1"])



    if __name__ == '__main__':
        unittest.main()
