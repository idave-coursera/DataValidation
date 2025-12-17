import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf
import yaml


def load_and_clean_jira_data(spark, jira_csv_path):
    if not os.path.exists(jira_csv_path):
        raise FileNotFoundError(f"File not found: {jira_csv_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(jira_csv_path)
    df_renamed = df.select(
        col("Custom field (EDW table)").alias("edw_table"),
        col("Custom field (EDS equivalent table)").alias("eds_equivalent_table"),
        col("Status").alias("status"),
        col("Custom field (EDW table replaced with external table?)").alias("is_table_replaced"),
        col("Issue key").alias("issue_key")
    )

    return df_renamed


def analyze_jira_tickets(df):
    total_count = df.count()
    status_counts_rows = df.groupBy("status").count().collect()
    status_counts = {row['status']: row['count'] for row in status_counts_rows}
    done_count = status_counts.get("Done", 0)
    wont_do_count = status_counts.get("Won't Do", 0)
    df_done = df.filter(col("status") == "Done").fillna("not_updated", subset=["is_table_replaced"])
    replaced_counts_rows = df_done.groupBy("is_table_replaced").count().collect()
    replaced_dict = {row['is_table_replaced']: row['count'] for row in replaced_counts_rows}
    replaced_count = replaced_dict.get("Yes", 0)
    not_replaced_count = replaced_dict.get("No", 0)
    replaced_status_unknown_count = replaced_dict.get("not_updated", 0)
    print(f"There are {total_count} tickets under EDSMIG-25,{done_count} done + {wont_do_count} marked won't do")
    print(
        f"{replaced_count} replaced , {not_replaced_count} not replaced and {replaced_status_unknown_count} with replacement status unknown")
    return df_done


def process_not_replaced_tables(df_done):
    df_done.select("is_table_replaced").distinct().show()
    df_done_not_replaced = df_done.filter(col("is_table_replaced") == "No")
    df_done_not_replaced = df_done_not_replaced.withColumn(
        "edw_table",
        regexp_replace(col("edw_table"), "prod", "coursera_warehouse.edw_prod")
    )
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


def loadIngestionKeys(spark):
    input_file = "resources/ingestionPath.txt"
    ans = []
    with open(input_file, "r") as file:
        paths = [line.strip() for line in file if line.strip()]
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


def join_with_ingestion_keys(spark, df_tickets, ingestion_keys_path):
    if not os.path.exists(ingestion_keys_path):
        raise FileNotFoundError(f"File not found: {ingestion_keys_path}")
    df_ik = loadIngestionKeys(spark)
    merged = (df_tickets
              .join(df_ik, col("eds_equivalent_table").endswith(concat(lit("."), col("table"), lit("_vw"))), "inner"))
    return merged


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("JiraAnalyser") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    try:
        pass
        df = load_and_clean_jira_data(spark, "./resources/jira_tickets.csv")
        df_done = analyze_jira_tickets(df)
        df_done_not_replaced = process_not_replaced_tables(df_done)
        final_result = join_with_ingestion_keys(spark, df_done_not_replaced, "./resources/ingestionKeys.csv")
        final_result.coalesce(1).write.mode("overwrite").json("./compared_tickets_spark_output.json")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        pass
        # spark.stop()
