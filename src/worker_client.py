import httpx
import asyncio
import random
import time
import argparse

async def worker(worker_id, num_requests, service_url):
    print(f"Worker {worker_id} starting...")
    async with httpx.AsyncClient() as client:
        for i in range(num_requests):
            payload = {
                "id": random.randint(1000, 9999),
                "partition": f"worker_{worker_id}",
                "reference": f"REQ-{worker_id}-{i}",
                "amount": round(random.uniform(1.0, 1000.0), 2),
                "status": "pending"
            }
            try:
                # Independent workers just 'fire and forget' to the service
                await client.post(f"{service_url}/ingest", json=payload)
            except Exception as e:
                print(f"Worker {worker_id} failed to send: {e}")
            
            # Optional: simulate some work
            # await asyncio.sleep(0.01)

    print(f"Worker {worker_id} finished {num_requests} writes.")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--reqs", type=int, default=100)
    args = parser.parse_args()

    url = "http://localhost:8000"
    tasks = [worker(i, args.reqs, url) for i in range(args.workers)]
    
    start = time.time()
    await asyncio.gather(*tasks)
    duration = time.time() - start
    
    print(f"\nDistributed benchmark finished.")
    print(f"Total rows sent: {args.workers * args.reqs}")
    print(f"Time taken to send: {duration:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())
