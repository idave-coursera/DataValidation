import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf
import pyspark.sql.functions as f
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
    df_done = (df.fillna("not_updated", subset=["is_table_replaced","eds_equivalent_table"])
               .filter(col("status") == "Done"))
    df_done_not_replaced_eds_updated = (df_done.filter(col("is_table_replaced").isin("No", "not_updated") )
                                        .filter(col("eds_equivalent_table") != "not_updated"))
    df_done_eds_name_updated = df_done.filter(col("eds_equivalent_table") != "not_updated")
    eds_name_not_updated_count = df_done.filter(col("eds_equivalent_table") == "not_updated").count()
    eds_name_updated_count = df_done_eds_name_updated.count()
    replaced_counts_rows = df_done_eds_name_updated.groupBy("is_table_replaced").count().collect()
    replaced_dict = {row['is_table_replaced']: row['count'] for row in replaced_counts_rows}
    replaced_count = replaced_dict.get("Yes", 0)
    not_replaced_count = replaced_dict.get("No", 0)
    replaced_status_unknown_count = replaced_dict.get("not_updated", 0)
    print("-" * 50)
    print(f"JIRA Analysis Summary (EDSMIG-25)")
    print("-" * 50)
    print(f"Total tickets: {total_count}")
    print(f"  • Done: {done_count}")
    print(f"  • Won't Do: {wont_do_count}")
    print(f"\nBreakdown of 'Done' tickets:")
    print(f"  • EDS Name not updated: {eds_name_not_updated_count}")
    print(f"  • EDS Name updated: {eds_name_updated_count}")
    print(f"  • Replaced: {replaced_count}")
    print(f"  • Not Replaced: {not_replaced_count}")
    print(f"  • Status Unknown: {replaced_status_unknown_count}")
    print(f"  • Done + EDS name available + unmapped pointback or no pointback: {df_done_not_replaced_eds_updated.count()}")
    print("-" * 50)
    return df_done_not_replaced_eds_updated


def add_cw_name(df_done):
    df_done_not_replaced = (
        df_done
        .withColumn(
            "edw_table", regexp_replace(col("edw_table"), "prod", "coursera_warehouse.edw_prod")
        )
        .withColumn("eds_base_table", col("eds_equivalent_table"))
        .withColumn("eds_base_table", regexp_replace("eds_base_table", "silver", "silver_base"))
        .withColumn("eds_base_table", regexp_replace("eds_base_table", "gold", "gold_base"))
        .withColumn("eds_base_table", regexp_replace("eds_base_table", "bronze", "bronze_base"))
        .withColumn("eds_base_table", regexp_replace("eds_base_table", "_vw", ""))
    )

    return df_done_not_replaced


def load_ingestion_keys(spark):
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
    df_ik = load_ingestion_keys(spark)
    merged = (df_tickets
              .join(df_ik, col("eds_equivalent_table").endswith(concat(lit("."), col("table"), lit("_vw"))), "left"))
    return merged

def analyse_pk_data(merged_df):
    total_joined = merged_df.count()
    
    # Tables that have PKs (where key_cols is not null)
    # Since we did a left join, if there was no match in ingestion keys, key_cols will be null
    with_pk_count = merged_df.filter(col("key_cols").isNotNull()).count()
    without_pk_count = total_joined - with_pk_count

    print("-" * 50)
    print("Primary Key Analysis Summary")
    print("-" * 50)
    print(f"Total tables analyzed: {total_joined}")
    print(f"  • With PK defined: {with_pk_count}")
    print(f"  • Without PK defined: {without_pk_count}")
    print("-" * 50)
    
    return merged_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("JiraAnalyser") \
        .getOrCreate()

    try:
        df = load_and_clean_jira_data(spark, "./resources/jira_tickets.csv")
        df_done = analyze_jira_tickets(df)
        df_done_not_replaced = add_cw_name(df_done)
        sortColumn = [
            when(col("key_cols").isNotNull(), 0).otherwise(1),
            when(col("is_table_replaced") == "No", 0).otherwise(1)
        ]
        final_result = join_with_ingestion_keys(spark, df_done_not_replaced, "./resources/ingestionKeys.csv").orderBy(*sortColumn)
        analyse_pk_data(final_result)
        with open('./output/bulk_validation_compare.json', 'w') as fp:
            json.dump(final_result.rdd.map(lambda row: row.asDict()).collect(), fp)
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        pass
        # spark.stop()
