import pandas as pd
import numpy as np
import os
import time

# Configuration
NUM_ROWS = 2_000_000  # 2 Million rows to make DuckDB work for it!
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ecommerce.parquet")

def generate_dataset():
    print(f"Generating {NUM_ROWS:,} rows of synthetic e-commerce data...")
    start_time = time.time()

    # Generate random data using numpy for extreme speed
    np.random.seed(42) 
    
    data = {
        "transaction_id": np.arange(1, NUM_ROWS + 1),
        "user_id": np.random.randint(1, 500_000, NUM_ROWS),
        "product_category": np.random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"], NUM_ROWS),
        "amount": np.round(np.random.uniform(5.0, 1500.0, NUM_ROWS), 2),
        "region": np.random.choice(["North", "South", "East", "West"], NUM_ROWS)
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Save to Parquet format
    print("Saving to Parquet format (optimized for DuckDB)...")
    df.to_parquet(OUTPUT_FILE, engine="pyarrow")

    elapsed = time.time() - start_time
    print(f"Success! Dataset created at '{OUTPUT_FILE}' in {elapsed:.2f} seconds.")
    
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"Total file size: {file_size_mb:.2f} MB")

if __name__ == "__main__":
    generate_dataset()