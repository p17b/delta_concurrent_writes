import argparse
import concurrent.futures
import time
import os
import sys

def run_writer(partition, delta_writer):
    # Capture output for metrics
    import io
    from contextlib import redirect_stdout
    
    f = io.StringIO()
    with redirect_stdout(f):
        try:
            delta_writer(partition)
        except Exception as e:
            print(f"Error: Partition {partition} failed: {e}")
    return f.getvalue().strip()

def main():
    # Ensure the root directory is in the path for imports
    root_dir = os.getcwd()
    if root_dir not in sys.path:
        sys.path.append(root_dir)
    
    try:
        from src.writer import writer as delta_writer
    except ImportError as e:
        print(f"Failed to import src.writer: {e}")
        print(f"Current sys.path: {sys.path}")
        return

    parser = argparse.ArgumentParser(description="Delta Concurrent Write Orchestrator")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent workers")
    parser.add_argument("--total-writes", type=int, default=10, help="Total number of writes to perform")
    args = parser.parse_args()

    concurrency = args.concurrency
    total_writes = args.total_writes
    print(f"Starting {total_writes} writes with {concurrency} concurrent workers (Thread-based)...")

    partitions = [f"bench_{i}" for i in range(total_writes)]
    
    start_total = time.time()
    
    # Increasing max_workers for scaling
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(concurrency, 500)) as executor:
        # Pass delta_writer to run_writer
        futures = {executor.submit(run_writer, p, delta_writer): p for p in partitions}
        
        results = []
        count = 0
        total = len(futures)
        progress_step = max(1, total // 10)
        
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            count += 1
            if count % progress_step == 0:
                print(f"Progress: {count}/{total} tasks completed ({(count/total)*100:.1f}%)")
    
    end_total = time.time()
    total_duration = end_total - start_total
    
    import re
    success_count = 0
    error_count = 0
    total_retries = 0
    total_wait_time = 0
    
    for r in results:
        if "Success" in r:
            success_count += 1
            match = re.search(r"retries: (\d+), wait: ([\d\.]+)s", r)
            if match:
                total_retries += int(match.group(1))
                total_wait_time += float(match.group(2))
        else:
            error_count += 1
    
    print("\n--- Results ---")
    print(f"Total duration: {total_duration:.2f}s")
    print(f"Writes/Second: {success_count/total_duration:.2f}")
    print(f"Successes: {success_count}")
    print(f"Failures: {error_count}")
    print(f"Total Retries: {total_retries}")
    print(f"Total Wait Time: {total_wait_time:.2f}s")
    print(f"Avg Wait Time/Success: {total_wait_time/max(1, success_count):.4f}s")
    
    if error_count > 0:
        print("\nSample Errors:")
        errors = [r for r in results if not "Success" in r]
        for e in errors[:5]:
            print(f"  - {e if e else 'Empty error message'}")

if __name__ == "__main__":
    main()
