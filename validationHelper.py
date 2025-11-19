from datetime import datetime
import pandas as pd
import sys


def print_and_run_query(ss, q, supress_output=False):
    if not supress_output:
        print(q)
    return ss.sql(q).toPandas()


def size_in_mb(obj):
    return round(sys.getsizeof(obj) / 1024 / 1024, 3)


class Table:
    """
    time_travel_ts: UTC
    """

    def __init__(self, name, pkey=[], ts_col=None, from_time_stamp=None, time_travel_ts=None, to_time_stamp=None,
                 ss=None, filter=None):
        self.name = name
        self.size = 0
        self.schema = pd.DataFrame()
        self.pkey = pkey
        self.pkey_aliases = [f"pkey{i}" for i in range(len(self.pkey))]
        self.pkey_aliases_clause = ",".join([f"{k} AS pkey{i}" for i, k in enumerate(self.pkey)])
        self.ss = ss
        self.filter = filter

        self.ts_col = ts_col
        self.from_time_stamp = from_time_stamp
        self.to_time_stamp = to_time_stamp
        self.ts_filter_clause = ""
        if self.ts_col is not None:
            if self.from_time_stamp is not None:
                self.ts_filter_clause = f"WHERE {self.ts_col} >= \"{self.from_time_stamp}\""
            if self.to_time_stamp is not None:
                if self.ts_filter_clause:
                    self.ts_filter_clause += f" AND {self.ts_col} <= \"{self.to_time_stamp}\""
                else:
                    self.ts_filter_clause = f"WHERE {self.ts_col} <= \"{self.to_time_stamp}\""
        if self.filter is not None:
            if self.ts_filter_clause:
                self.ts_filter_clause += f" AND {self.filter}"
            else:
                self.ts_filter_clause = f"WHERE {self.filter}"

        print(f"filter clause is {self.ts_filter_clause}")

        self.time_travel_ts = time_travel_ts
        if self.time_travel_ts is not None:
            time_travel_ts_str = self.time_travel_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
            self.time_travel_clause = f"TIMESTAMP AS OF \"{time_travel_ts_str}\""
        else:
            self.time_travel_clause = ""

        q = f"""
      SELECT COUNT(*) cnt
      FROM {self.name} {self.time_travel_clause}
      {self.ts_filter_clause};
    """
        res = print_and_run_query(self.ss, q)
        self.size = res['cnt'].iloc[0]

        q = f"DESCRIBE {self.name};"
        self.schema = print_and_run_query(self.ss, q, True)[['col_name', 'data_type']]

    def query_table(self, query):
        """
        Write the SQL query using {tb} as the placeholder for your table.
        """
        base_query = f"""
      WITH base AS(
      SELECT *
      FROM {self.name} {self.time_travel_clause}
      {self.ts_filter_clause}
      )
    """
        complete_query = base_query + query
        complete_query = complete_query.replace("{tb}", "base")
        print(complete_query)
        return self.ss.sql(complete_query)

    def run_primary_key_check(self):
        q = f"""
    WITH pkey_counts as
    (
        SELECT {self.pkey_aliases_clause}, COUNT(*) entry_count
        FROM {self.name} {self.time_travel_clause}
        {self.ts_filter_clause}
        GROUP BY {",".join(self.pkey_aliases)}
    )
    SELECT * FROM pkey_counts
    WHERE entry_count > 1;
    """
        print(f"Running primary key check for {self.name} for primary keys {self.pkey}")
        res = print_and_run_query(self.ss, q, True)

        if res.size == 0:
            msg = f"Primary key check passed."
            print(msg)
            return 0, msg, pd.DataFrame()
        else:
            msg = f"Primary key check failed, with {res.size} duplicated primary keys."
            print(msg)
            return 1, msg, res


class Validator:
    def __init__(self, edw_table: Table, eds_table: Table, ss):
        self.edw_table = edw_table
        self.eds_table = eds_table
        self.ss = ss
        self.report = ""

        self.shared_schema = pd.merge(edw_table.schema, eds_table.schema)
        print(f"Generated shared schema based on column names and datatypes:")
        print(self.shared_schema)

        print(f"\nThe following columns have been excluded from the EDW table:")
        print(edw_table.schema.merge(eds_table.schema, indicator=True, how='left').query('_merge == "left_only"').drop(
            '_merge', axis=1))

        print(f"\nThe following columns have been excluded from the EDS table:")
        print(eds_table.schema.merge(edw_table.schema, indicator=True, how='left').query('_merge == "left_only"').drop(
            '_merge', axis=1))

    """
      Returns any pkeys which exist in EDW that are missing from EDS, if base_system='edw'.
      Returns any pkeys which exist in EDS that are missing from EDW, if base_system='eds'.
  
      base_system is either 'edw' or 'eds'
    """

    def run_pkey_existence_check(self, base_system='edw'):
        if base_system.lower() == 'edw':
            base_table = self.edw_table
            comp_system = 'eds'
            comp_table = self.eds_table
        elif base_system.lower() == 'eds':
            base_table = self.eds_table
            comp_system = 'edw'
            comp_table = self.edw_table
        else:
            raise ValueError(f"Base system {base_system} not recognized! Must be one of 'edw' or 'eds'.")

        q = f"""
    WITH pkeys_base as
    (
      SELECT {base_table.pkey_aliases_clause}
      FROM {base_table.name} {base_table.time_travel_clause}
      {base_table.ts_filter_clause}
      GROUP BY {",".join(base_table.pkey_aliases)}
    ),
    pkeys_comp as
    (
      SELECT {comp_table.pkey_aliases_clause}
      FROM {comp_table.name} {comp_table.time_travel_clause}
      {comp_table.ts_filter_clause}
      GROUP BY {",".join(comp_table.pkey_aliases)}
    )
    SELECT * FROM pkeys_base
    MINUS
    SELECT * FROM pkeys_comp
    """
        missing_records_res = print_and_run_query(self.ss, q, True)
        record_count = len(missing_records_res)
        record_pct = round(100 * record_count / base_table.size, 2)

        if record_count == 0:
            msg = f"No pkeys that exist in {base_system.upper()} are missing from {comp_system.upper()}."
            print(msg)
            return 0, msg, missing_records_res
        else:

            msg = f"{record_count} records ({record_pct}%) that exist in {base_system.upper()} are missing from {comp_system.upper()} (i.e. {base_system.upper()} contains records that do not exist in {comp_system.upper()}). This may be because {base_system.upper()} data is fresher than {comp_system.upper()} data, or there is a logical error in the workflow which populates the table."
            print(msg)
            return 1, msg, missing_records_res

    def run_column_spot_check(self, sample_pct, truncate_ts=False):
        if not (0 <= sample_pct <= 100):
            raise ValueError(f"sample_pct must be between 0 and 100, got {sample_pct}")

        limit_val = int(self.edw_table.size / 100 * sample_pct)

        # Test query to generate upper limit of memory footprint
        q = f"""
    SELECT *
    FROM {self.eds_table.name} {self.eds_table.time_travel_clause}
    LIMIT 100;
    """
        res = print_and_run_query(self.ss, q, True)
        sizeof_100_rows_in_mb = size_in_mb(res)
        sizeof_all_rows_in_mb = sizeof_100_rows_in_mb * limit_val / 100

        # This is an estimate of memory, and its value may change if the
        # implementation of this method changes
        total_memory_footprint_in_mb = sizeof_all_rows_in_mb * 4

        print(
            f"WARN: With a sample size of {sample_pct}% ({limit_val} records), running this function may take up to {total_memory_footprint_in_mb} MB. Verify that your compute configuration has more than enough memory to handle this workload, otherwise lower the sample size.")

        # Generate sample of pkeys
        q = f"""
    SELECT {self.edw_table.pkey_aliases_clause}
    FROM {self.edw_table.name} {self.edw_table.time_travel_clause}
    {self.edw_table.ts_filter_clause}
    ORDER BY RAND()
    LIMIT {limit_val}
    """
        res = print_and_run_query(self.ss, q, True)

        print(f"pkeys df size: {size_in_mb(res)} MB")

        self.ss.createDataFrame(res).createOrReplaceTempView("sample_primary_keys")

        # Get all records from EDS table with sample pkeys
        q = f"""
    SELECT * FROM
    (
    SELECT * FROM {self.eds_table.name} {self.eds_table.time_travel_clause} 
    {self.eds_table.ts_filter_clause} ) eds
    INNER JOIN sample_primary_keys s
      ON {" AND ".join([f"eds.{self.eds_table.pkey[i]} = s.pkey{i}" for i in range(len(self.eds_table.pkey))])}
    """
        eds_res = print_and_run_query(self.ss, q, True)

        print(f"eds df size: {size_in_mb(eds_res)} MB")

        # Get all records from EDW table
        q = f"""
    SELECT * FROM
    (SELECT * FROM {self.edw_table.name} {self.edw_table.time_travel_clause} {self.edw_table.ts_filter_clause}) edw
    INNER JOIN sample_primary_keys s
      ON {" AND ".join([f"edw.{self.edw_table.pkey[i]} = s.pkey{i}" for i in range(len(self.edw_table.pkey))])}
    """
        edw_res = print_and_run_query(self.ss, q, True)

        print(f"edw df size: {size_in_mb(edw_res)} MB")

        # For each column, compare values between EDW and EDS, and store differing results in a dict
        column_differences = {}

        joined = pd.merge(edw_res, eds_res, on=self.edw_table.pkey_aliases, suffixes=('__edw', '__eds'))

        print(f"joined df size: {size_in_mb(joined)} MB")

        for row in self.shared_schema.itertuples():
            col_name, data_type = row[1], row[2]
            print(col_name, data_type)
            cols = joined[self.edw_table.pkey_aliases + [col_name + '__edw', col_name + '__eds']]
            if truncate_ts and data_type == 'timestamp':
                diffs = cols[pd.to_datetime(cols[col_name + '__edw']).dt.floor('S') != pd.to_datetime(
                    cols[col_name + '__eds']).dt.floor('S')]
            else:
                diffs = cols[cols[col_name + '__edw'] != cols[col_name + '__eds']]
            # required since NaN != NaN and NaT != NaT
            diffs = diffs[diffs[col_name + '__edw'].notnull() | diffs[col_name + '__eds'].notnull()]

            column_differences[col_name] = diffs

        sample_size = len(joined)

        return column_differences, sample_size