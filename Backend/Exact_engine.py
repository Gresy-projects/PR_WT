import duckdb
import time
import os

class ExactEngine:
    def __init__(self, db_path="data/ecommerce.parquet"):
        self.con = duckdb.connect(database=':memory:')
        
        # BULLETPROOF PATH LOGIC: Get the exact folder this script is in
        current_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(current_dir, "data", "ecommerce.parquet")
        
        try:
            self.con.execute(f"CREATE VIEW transactions AS SELECT * FROM read_parquet('{full_path}')")
            print(f"DuckDB Successfully loaded data from: {full_path}")
        except duckdb.Error as e:
            print(f"Warning: Could not load dataset at {full_path}. Error: {e}")

    def run_query(self, query_type, column, group_by=None):
        if group_by:
            sql = f"SELECT {group_by}, {query_type}({column}) FROM transactions GROUP BY {group_by}"
        else:
            sql = f"SELECT {query_type}({column}) FROM transactions"

        start_time = time.perf_counter()
        raw_result = self.con.execute(sql).fetchall()
        end_time = time.perf_counter()
        
        execution_time_ms = (end_time - start_time) * 1000  
        
        if not group_by and raw_result:
            result_value = raw_result[0][0]
        else:
            result_value = raw_result

        return result_value, execution_time_ms