import pandas as pd
import time
import os
from deltalake import write_deltalake
from datetime import datetime
import random

def get_table_path():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "warehouse", "delta_table_batched")

def generate_data(num_rows, batch_id):
    return pd.DataFrame([{
        "id": i,
        "partition": f"batch_{batch_id}",
        "created_at": datetime.now(),
        "created_partition": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "reference": f"REF-{i}-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(10.0, 500.0), 2),
        "status": "completed"
    } for i in range(num_rows)])

def benchmark_unbatched(total_rows):
    print(f"\n--- Unbatched Benchmark: {total_rows} transactions ---")
    path = get_table_path()
    start = time.time()
    for i in range(total_rows):
        data = generate_data(1, i)
        write_deltalake(path, data, mode="append", partition_by="partition")
        if (i+1) % 10 == 0:
            print(f"Processed {i+1}/{total_rows} writes...", end="\r")
    
    duration = time.time() - start
    print(f"\nTotal time: {duration:.2f}s")
    print(f"Throughput: {total_rows/duration:.2f} writes/s")

def benchmark_batched(total_rows):
    print(f"\n--- Batched Benchmark: 1 transaction for {total_rows} rows ---")
    path = get_table_path()
    
    # Simulate collecting data in a buffer
    start_buffer = time.time()
    buffer = []
    for i in range(total_rows):
        buffer.append({
            "id": i,
            "partition": "batched_single",
            "created_at": datetime.now(),
            "created_partition": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "reference": f"REF-{i}-BATCH",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": "completed"
        })
    df = pd.DataFrame(buffer)
    buffer_time = time.time() - start_buffer
    
    # Single Write
    start_write = time.time()
    write_deltalake(path, df, mode="append", partition_by="partition")
    write_time = time.time() - start_write
    
    total_time = buffer_time + write_time
    print(f"Buffer time: {buffer_time:.4f}s")
    print(f"Write time: {write_time:.4f}s")
    print(f"Total time: {total_time:.2f}s")
    print(f"Effective Throughput: {total_rows/total_time:.2f} writes/s")

if __name__ == "__main__":
    total = 100
    print(f"Comparing performance for {total} operations...")
    
    # Run Batched first to initialize table if needed
    benchmark_batched(total)
    
    # Run Unbatched
    benchmark_unbatched(total)
