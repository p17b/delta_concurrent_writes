from time import strftime
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import random
from datetime import datetime
import argparse


import os
import time

def writer(partition):
    # Use absolute path for reliability
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    table_path = os.path.join(base_dir, "warehouse", "delta_table")
    
    start_time = time.time()
    max_retries = 10
    retry_count = 0
    total_wait_time = 0
    
    while retry_count <= max_retries:
        try:
            data = pd.DataFrame([{
                "id": i,
                "partition": partition,
                "created_at": datetime.now(),
                "created_partition": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "reference": f"REF-{i}-{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "amount": round(random.uniform(10.0, 500.0), 2),
                "status": random.choice(["pending", "completed", "failed"])
            } for i in range(10)])

            write_deltalake(
                table_or_uri=table_path, 
                data=data, 
                partition_by='partition',
                mode="append",
                schema_mode="merge"
            )
            duration = time.time() - start_time
            print(f"Success: Partition {partition} written in {duration:.2f}s (retries: {retry_count}, wait: {total_wait_time:.2f}s)")
            return
        except Exception as e:
            if "Conflict" in str(e) or "commit transaction" in str(e) or "protocol" in str(e):
                retry_count += 1
                if retry_count > max_retries:
                    duration = time.time() - start_time
                    print(f"Error: Partition {partition} failed after {max_retries} retries in {duration:.2f}s: {e}")
                    return
                # Exponential backoff with jitter
                wait = (2 ** retry_count) * 0.1 * (1 + random.random())
                total_wait_time += wait
                time.sleep(wait)
            else:
                duration = time.time() - start_time
                print(f"Error: Partition {partition} failed with non-retryable error after {duration:.2f}s: {e}")
                return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write data to a delta table")
    parser.add_argument("--partition", type=str, required=True)
    args = parser.parse_args()
    writer(args.partition)
    
