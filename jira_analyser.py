import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf
import pyspark.sql.functions as f
import yaml
from pyspark.sql import Window
from config import BASE_PATH
from validationUtils import ts


def load_jira_data(spark, jira_csv_path):
    if not os.path.exists(jira_csv_path):
        raise FileNotFoundError(f"File not found: {jira_csv_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(jira_csv_path)
    eds_schema_col_expr = f.split(col("eds_equivalent_table"), r"\.")
    renamed_jira_df = df.select(
        col("Custom field (EDW table)").alias("edw_table"),
        col("Custom field (EDS equivalent table)").alias("eds_equivalent_table"),
        col("Status").alias("status"),
        col("Custom field (EDW table replaced with external table?)").alias("is_table_replaced"),
        col("Issue key").alias("issue_key"),
        when(col("eds_equivalent_table").isNotNull() & (f.size(eds_schema_col_expr) > 1), (eds_schema_col_expr.getItem(1))).alias("eds_schema")
        )

    return renamed_jira_df


def analyze_jira_tickets(df):
    total_count = df.count()
    status_counts = dict(df.groupBy("status").count().rdd.collectAsMap())
    
    # Pre-filter common datasets
    done_tickets_df = df.fillna("not_updated", subset=["is_table_replaced", "eds_equivalent_table"]).filter(col("status") == "Done")
    done_eds_updated_df = done_tickets_df.filter(col("eds_equivalent_table") != "not_updated")
    
    # Calculate detailed breakdowns
    eds_updated_count = done_eds_updated_df.count()
    replaced_counts = done_eds_updated_df.groupBy("is_table_replaced").count().rdd.collectAsMap()
    
    target_processing_df = (done_tickets_df
        .filter(col("is_table_replaced").isin("No", "not_updated"))
        .filter(col("eds_equivalent_table") != "not_updated")
        .filter(~col("eds_equivalent_table").contains("static")))

    print("-" * 50)
    print("JIRA Analysis Summary")
    print("-" * 50)
    print(f"Total tickets: {total_count}")
    print(f"  • Done: {status_counts.get('Done', 0)}")
    print(f"  • Won't Do: {status_counts.get('Won\'t Do', 0)}")
    print("\nBreakdown of 'Done' tickets:")
    print(f"  • EDS Name not updated: {done_tickets_df.filter(col('eds_equivalent_table') == 'not_updated').count()}")
    print(f"  • EDS Name updated: {eds_updated_count}")
    print(f"  • Replaced: {replaced_counts.get('Yes', 0)}")
    print(f"  • Not Replaced: {replaced_counts.get('No', 0)}")
    print(f"  • Status Unknown: {replaced_counts.get('not_updated', 0)}")
    print(f"  • Target for processing: {target_processing_df.count()}")
    print("-" * 50)

    return target_processing_df


def add_coursera_warehouse_prefix(df):
    edw_schema_col_expr = f.concat(lit("coursera_warehouse.edw_"), col("edw_table"))
    cw_prefixed_df = df.withColumn("edw_table", edw_schema_col_expr)
    return cw_prefixed_df

def standardize_eds_names(df):
    replacements = {
        "silver": "silver_base",
        "gold": "gold_base",
        "bronze": "bronze_base",
        "_vw$": ""
    }
    
    # Initialize column
    standardized_eds_df = df.withColumn("eds_base_table", col("eds_equivalent_table"))
    
    # Apply replacements
    for pattern, replacement in replacements.items():
        standardized_eds_df = standardized_eds_df.withColumn(
            "eds_base_table", regexp_replace("eds_base_table", pattern, replacement)
        )
    return standardized_eds_df


def load_ingestion_keys(spark):
    input_file = "resources/ingestionPath.txt"
    ans = []
    with open(input_file, "r") as file:
        paths = [BASE_PATH+line.strip() for line in file if line.strip()]
    for path in paths:
        with open(path, "r") as f2:
            yobj = yaml.safe_load(f2).get('configurations', [])
            for table in yobj[1:]:
                kcg = table.get("key_columns")
                kc = []
                if type(kcg) == dict:
                    kc = kcg["snapshots"]
                elif type(kcg) == list:
                    kc = kcg
                ans.append((table.get("target_table", "NO TABLE"), kc))
    return spark.createDataFrame(ans, ["table", "key_cols"])

def load_view_keys(spark):
    input_file = "resources/view_keys.json"
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return None
        
    with open(input_file, 'r') as f:
        data = json.load(f)
        
    # Convert dict to list of tuples for DataFrame creation
    # data is like: {"schema.table": ["col1", "col2"], ...}
    rows = []
    for table_name, keys in data.items():
        rows.append((table_name, keys))
        
    if not rows:
        print("No keys found in view_keys.json")
        return spark.createDataFrame([], schema="table string, view_key_cols array<string>")

    return spark.createDataFrame(rows, ["view_table", "view_key_cols"])


def join_with_keys(spark, df_tickets_df):
    ingestion_keys_df = load_ingestion_keys(spark)
    view_keys_df = load_view_keys(spark)
    merged_df = ((df_tickets_df
              .join(ingestion_keys_df, col("eds_equivalent_table").endswith(concat(lit("."), col("table"), lit("_vw"))), "left"))
              .join(view_keys_df , col("eds_equivalent_table").contains(col("view_table")), "left")
                 .withColumn("primary_keys" , f.coalesce(col("key_cols"),col("view_key_cols"))).drop("key_cols","view_key_cols"))

    return merged_df

def analyse_pk_data(merged_df):
    total_joined = merged_df.count()

    # Tables that have PKs (where primary_keys is not null)
    # Since we did a left join, if there was no match in ingestion keys, primary_keys will be null
    with_pk_count = merged_df.filter(col("primary_keys").isNotNull()).count()
    without_pk_count = total_joined - with_pk_count

    print("-" * 50)
    print("Primary Key Analysis Summary")
    print("-" * 50)
    print(f"Total tables analyzed: {total_joined}")
    print(f"  • With PK defined: {with_pk_count}")
    print(f"  • Without PK defined: {without_pk_count}")
    print("-" * 50)

    return merged_df

def write_output(final_result_df):
    w = Window.partitionBy("eds_schema").orderBy("issue_key")
    batched_result_df = final_result_df.withColumn("batch_id",(f.row_number().over(w) / 100).cast("int"))
    schema_batches_df = batched_result_df.select("eds_schema", "batch_id").distinct()
    schema_batch_list = [(x["eds_schema"], x["batch_id"] )for x in schema_batches_df.collect() if "/" not in x["eds_schema"]]
    sort_keys = [
        when(col("primary_keys").isNotNull(), 0).otherwise(1),
        when(col("is_table_replaced") == "No", 0).otherwise(1)
    ]
    for schema, batch_id in schema_batch_list:
        with open(f'./output/{schema}_{batch_id}_bulk_validation_results.json', 'w') as fp:
            json.dump(batched_result_df.filter((col("eds_schema") == schema) & (col("batch_id") == batch_id)).orderBy(*sort_keys).rdd.map(lambda row: row.asDict()).collect(), fp,indent=4)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("JiraAnalyser") \
        .getOrCreate()

    try:
        jira_tickets_df = load_jira_data(spark, "./resources/jira_migration_tickets.csv")
        done_tickets_with_updated_eds_df = analyze_jira_tickets(jira_tickets_df)
        
        tickets_with_cw_name_df = add_coursera_warehouse_prefix(done_tickets_with_updated_eds_df)
        tickets_with_standardized_eds_df = standardize_eds_names(tickets_with_cw_name_df)
        validated_tickets_with_keys_df = join_with_keys(spark, tickets_with_standardized_eds_df)
        
        analyse_pk_data(validated_tickets_with_keys_df)
        write_output(validated_tickets_with_keys_df)
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        pass
        # spark.stop()
