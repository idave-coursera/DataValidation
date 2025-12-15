import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf
from pyspark.sql.types import StringType

def load_and_clean_jira_data(spark, jira_csv_path):
    if not os.path.exists(jira_csv_path):
        raise FileNotFoundError(f"File not found: {jira_csv_path}")

    # Load only necessary columns. Spark reads all by default then we select.
    # We rename columns to be friendly.
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(jira_csv_path)
    
    # Selecting and renaming specific columns
    # Note: Spark column names with spaces/parens need backticks or col()
    df_renamed = df.select(
        col("Custom field (EDW table)").alias("edw_table"),
        col("Custom field (EDS equivalent table)").alias("eds_equivalent_table"),
        col("Status"),
        col("Custom field (EDW table replaced with external table?)").alias("is_table_replaced"),
        col("Issue id").alias("Issue id")
    )
    
    return df_renamed

def analyze_jira_tickets(df):
    total_count = df.count()
    print(f"There are {total_count} tickets under EDSMIG-25")

    # Status counts
    # groupBy and count
    status_counts_rows = df.groupBy("Status").count().collect()
    status_counts = {row['Status']: row['count'] for row in status_counts_rows}
    
    done_count = status_counts.get("Done", 0)
    wont_do_count = status_counts.get("Won't Do", 0)
    
    print(f"Out of those, {done_count} done + {wont_do_count} marked won't do")

    # Filter for Done
    df_done = df.filter(col("Status") == "Done").fillna("not_updated", subset=["is_table_replaced"])
    
    # Replaced status counts
    replaced_counts_rows = df_done.groupBy("is_table_replaced").count().collect()
    replaced_dict = {row['is_table_replaced']: row['count'] for row in replaced_counts_rows}
    
    replaced_count = replaced_dict.get("Yes", 0)
    not_replaced_count = replaced_dict.get("No", 0)
    replaced_status_unknown_count = replaced_dict.get("not_updated", 0)

    print("We need tables which have not been replaced with external table, there are")
    print(f"{replaced_count} replaced , {not_replaced_count} not replaced and {replaced_status_unknown_count} with replacement status unknown")
    print("The analysis is only valid for tables that have not been replaced with external table, since post replacement EDS and EDW match one to one")
    
    return df_done

def process_not_replaced_tables(df_done):
    df_done_not_replaced = df_done.filter(col("is_table_replaced") == "No")
    
    # Replacement logic using regexp_replace
    # Replace "prod" with "coursera_warehouse.edw_prod" in edw_table
    df_done_not_replaced = df_done_not_replaced.withColumn(
        "edw_table", 
        regexp_replace(col("edw_table"), "prod", "coursera_warehouse.edw_prod")
    )
    
    # Chained replacement for eds_base_table derived from eds_equivalent_table
    df_done_not_replaced = df_done_not_replaced.withColumn(
        "eds_base_table",
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("eds_equivalent_table"), "silver", "silver_base"),
                    "gold", "gold_base"
                ),
                "bronze", "bronze_base"
            ),
            "_vw", ""
        )
    )
    
    return df_done_not_replaced

def join_with_ingestion_keys(spark, df_tickets, ingestion_keys_path):
    if not os.path.exists(ingestion_keys_path):
         raise FileNotFoundError(f"File not found: {ingestion_keys_path}")

    # Load ingestion keys
    # CSV has standard header: ,table,key_cols
    # "key_cols" might be a string representation of a list "['a','b']". 
    # For now we treat it as string since we just need "table" for the join logic in this specific step.
    df_ik = spark.read.option("header", "true").csv(ingestion_keys_path).select("table", "key_cols")
    
    # Cross join strategy
    # Spark's crossJoin
    merged = df_tickets.crossJoin(df_ik)
    
    # Filter based on suffix match: eds_equivalent_table ends with ".{table}_vw"
    # Logic: eds_equivalent_table.endswith("." + table + "_vw")
    
    result_df = merged.filter(
        col("eds_equivalent_table").endswith(concat(lit("."), col("table"), lit("_vw")))
    )
    
    return result_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("JiraAnalyser") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    try:
        df = load_and_clean_jira_data(spark, "./resources/jira_tickets.csv")
        df_done = analyze_jira_tickets(df)
        df_done_not_replaced = process_not_replaced_tables(df_done)
        
        print("Data Frame after processing:")
        df_done_not_replaced.show(truncate=False)

        final_result = join_with_ingestion_keys(spark, df_done_not_replaced, "./resources/ingestionKeys.csv")
        
        print("\nRows with missing table match (null table):")
        final_result.filter(col("table").isNull()).show()
        
        # Save to CSV (coalesce to 1 to get single file output similar to pandas to_csv)
        final_result.write.mode("overwrite").option("header", "true").csv("./compared_tickets_spark_output")
        print("\nSaved compared tickets to ./compared_tickets_spark_output")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        pass
        # spark.stop()
