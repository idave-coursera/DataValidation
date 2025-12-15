import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def process_ingestion_paths(spark, input_file="resources/ingestionPath.txt"):
    tables = []
    key_cols_data = []
    
    with open(input_file, "r") as f:
        paths = [line.strip() for line in f if line.strip()]
    for path in paths:
        try:
            with open(path, "r") as f2:
                config_data = yaml.safe_load(f2)
                configurations = config_data.get('configurations', [])
                
                # Skipping index 0 as per original logic (range(1, len))
                for config in configurations[1:]:
                    tables.append(config.get('target_table'))
                    
                    key_columns = config.get('key_columns')
                    if key_columns:
                        if isinstance(key_columns, list):
                            key_cols_data.append(key_columns)
                        elif isinstance(key_columns, dict) and 'snapshots' in key_columns:
                            key_cols_data.append(key_columns['snapshots'])
                        else:
                            key_cols_data.append(None)
                    else:
                        key_cols_data.append(None)
                        
        except FileNotFoundError:
            print(f"Warning: File not found: {path}")
        except Exception as e:
            print(f"Error processing {path}: {e}")

    # Create tuples for the dataframe
    data = [(t, k) for t, k in zip(tables, key_cols_data)]
    
    # Define schema explicitly to handle potential type inference issues with empty lists or None
    schema = StructType([
        StructField("table", StringType(), True),
        StructField("key_cols", ArrayType(StringType()), True)
    ])
    
    return spark.createDataFrame(data, schema)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IngestionPKFinder").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = process_ingestion_paths(spark)
    df.printSchema()
    df.show(truncate=False)
    
    # spark.stop() is usually good practice, but in some interactive environments it might kill the session.
    # spark.stop()
