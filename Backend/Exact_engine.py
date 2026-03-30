import duckdb
import time

class ExactEngine:
    def __init__(self, db_path="data/ecommerce.parquet"):
        self.con = duckdb.connect(database=':memory:')
        # Using a try-except here so it doesn't crash if the data file isn't generated yet!
        try:
            self.con.execute(f"CREATE VIEW transactions AS SELECT * FROM read_parquet('{db_path}')")
        except duckdb.Error:
            print(f"Warning: Dataset '{db_path}' not found yet. Generate data first!")

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