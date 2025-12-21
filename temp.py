import functools
import itertools

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, concat, count, array_contains, udf
import pyspark.sql.functions as f

from jira_analyser import load_jira_data

spark = SparkSession.builder \
        .appName("temp") \
        .getOrCreate()

df = load_jira_data(spark, "resources/jira_migration_tickets.csv")
df2 = df.filter(col("eds_equivalent_table").isNotNull()).filter(col("eds_equivalent_table").rlike("^[^p]"))
df2.show(400,False)
lis = df2.collect()
print([r['issue_key'] for r in lis])
