import unittest
from unittest.mock import MagicMock, patch
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

        # EDW Schema: pk_1, pk_2, common_1, common_2, common_3, common_4 (int), edw_only
        edw_schema = StructType([
            StructField("pk_1", IntegerType(), True),
            StructField("pk_2", StringType(), True),
            StructField("common_1", ArrayType(StringType()), True),
            StructField("common_2", common_2_struct, True),
            StructField("common_3", MapType(StringType(), IntegerType()), True),
            StructField("common_4", IntegerType(), True),
            StructField("edw_only", StringType(), True)
        ])

        # EDS Schema: pk_1, pk_2, common_1, common_2, common_3, common_4 (string), eds_only
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

    def test_ts(self):
        # Just checking it doesn't fail and returns the DF
        res = ts(self.df_edw)
        self.assertEqual(res, self.df_edw)

    def test_temp_dev_name(self):
        self.assertEqual(temp_dev_name("catalog.schema.table"), "dev.general.schema_table_cw")
        self.assertEqual(temp_dev_name("invalid_format"), -1)

    @patch('validationUtils.spark')
    def test_create_sample_df(self, mock_spark):
        # Mock the chain: spark.read.table(...).select(...).limit(...).collect()
        mock_row = MagicMock()
        # Mock sys.getsizeof via a dummy object or just ensure mocked row behaves
        # The code uses row.asDict(), so we mock that
        mock_row.asDict.return_value = {"col": "x" * 100} 
        
        mock_df = MagicMock()
        mock_df.limit.return_value.collect.return_value = [mock_row] * 10
        
        mock_spark.read.table.return_value.select.return_value = mock_df
        
        # Execute
        res = create_sample_df("cat.sch.tbl", ["col"], 1)
        
        # Verify
        self.assertEqual(res, "dev.general.sch_tbl_cw")
        mock_spark.sql.assert_called_once()
        args, _ = mock_spark.sql.call_args
        self.assertIn("CREATE OR REPLACE TABLE", args[0])

    def test_df_shared_schema(self):
        shared = df_shared_schema(self.df_edw, self.df_eds)
        # Expected shared: pk_1, pk_2, common_1, common_2, common_3
        # common_4 has different types (int vs string), so not shared
        expected_keys = {"pk_1", "pk_2", "common_1", "common_2", "common_3"}
        self.assertEqual(set(shared.keys()), expected_keys)
        self.assertEqual(shared["pk_1"], IntegerType())

    def test_compare_schemas(self):
        result = compare_schemas(self.df_edw, self.df_eds)
        
        # 1. df1_only (columns in df1 not in df2 by name)
        # edw_only is in df1, not in df2
        self.assertIn("edw_only", result["df1_only"])
        self.assertEqual(len(result["df1_only"]), 1)
        
        # 2. df2_only (columns in df2 not in df1 by name)
        # eds_only is in df2, not in df1
        self.assertIn("eds_only", result["df2_only"])
        self.assertEqual(len(result["df2_only"]), 1)
        
        # 3. same_types (common name, same type)
        expected_same = {"pk_1", "pk_2", "common_1", "common_2", "common_3"}
        self.assertEqual(set(result["same_types"].keys()), expected_same)
        
        # 4. diff_types (common name, different type)
        # common_4 is int in df1, string in df2
        self.assertIn("common_4", result["diff_types"])
        self.assertEqual(result["diff_types"]["common_4"], (IntegerType(), StringType()))

    def test_hash_df(self):
        # We test that it adds a hash column and keeps requested columns
        res = hash_df(self.df_edw, ["pk_1", "pk_2"], ["common_4"])
        
        self.assertIn("hash", res.columns)
        self.assertIn("common_4", res.columns)
        # Should not have other columns unless requested (only common_4 + hash)
        self.assertEqual(len(res.columns), 2) 
        
        # Check that hash is computed (non-null)
        rows = res.collect()
        self.assertIsNotNone(rows[0]['hash'])

    def test_compareDFs(self):
        # compareDFs(df1, df2, pk_cols, columns_to_compare)
        # It performs a join and transforms (shows).
        # We test it runs successfully.
        
        # Use columns that are shared
        try:
            compareDFs(self.df_edw, self.df_eds, pk_cols=["pk_1"], columns_to_compare=["common_2"])
        except Exception as e:
            self.fail(f"compareDFs raised exception: {e}")

        # Test with non-shared column should raise ValueError
        with self.assertRaises(ValueError):
            compareDFs(self.df_edw, self.df_eds, pk_cols=["pk_1"], columns_to_compare=["common_4"])


if __name__ == '__main__':
    unittest.main()
