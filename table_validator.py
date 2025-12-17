from pyspark.sql import SparkSession

def validate_table_schema(row, spark, broken_tables):
    """
    1. Loads three dataframes for eds, edw and eds_base.
    2. Checks if the 3 df.cols have the key columns.
    If df.columns access fails, adds the eds table name to broken_tables.
    """
    eds_table = row.get('eds_equivalent_table')
    edw_table = row.get('edw_table')
    base_table = row.get('eds_base_table')
    key_cols = row.get('key_cols', [])

    df_eds = None
    df_edw = None
    df_base = None

    # 1. Load three dataframes in a try statement
    try:
        if eds_table:
            df_eds = spark.table(eds_table)
        if edw_table:
            df_edw = spark.table(edw_table)
        if base_table:
            df_base = spark.table(base_table)
    except Exception:
        # "otherwise the df is None" - they remain None if initialization fails
        # or we could explicitly set them to None here if partial success is possible
        # but for simplicity, we rely on the initial None assignment
        pass

    # 2. Check if the 3 df.cols have the key columns
    try:
        # Check if all keys exist in columns for each dataframe
        # This will raise AttributeError if any df is None
        eds_ok = all(k in df_eds.columns for k in key_cols)
        edw_ok = all(k in df_edw.columns for k in key_cols)
        base_ok = all(k in df_base.columns for k in key_cols)

        # Return boolean indicating if all tables have valid keys
        return eds_ok and edw_ok and base_ok

    except Exception:
        # "if df.columns try fails add the eds table name to another list called broken_tables"
        if eds_table:
            broken_tables.append(eds_table)
        return False

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TableValidator") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # Schema from the prompt
    schema_row = {
        'edw_table': 'coursera_warehouse.edw_prod.branch_current_grades_v2', 
        'eds_equivalent_table': 'prod.silver.kinesis_branch_grades_latest_vw', 
        'Status': 'Done', 
        'is_table_replaced': 'No', 
        'issue_key': 'EDSMIG-5256', 
        'eds_base_table': 'prod.silver_base.kinesis_branch_grades_latest', 
        'table': 'kinesis_branch_grades_latest', 
        'key_cols': ['course_branch_id', 'user_id']
    }

    broken_tables = []
    
    print(f"Validating schema: {schema_row.get('table')}")
    result = validate_table_schema(schema_row, spark, broken_tables)
    
    print(f"Validation passed: {result}")
    print(f"Broken tables: {broken_tables}")

