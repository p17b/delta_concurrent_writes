import asyncio
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, Request
from deltalake import write_deltalake
import os
import json
from datetime import datetime
import uvicorn
import aiosqlite
from contextlib import asynccontextmanager

# Configuration
TABLE_PATH = os.path.join(os.getcwd(), "warehouse", "delta_table_reliable")
DB_PATH = os.path.join(os.getcwd(), "warehouse", "ingestion_buffer.db")
FLUSH_INTERVAL_SECONDS = 5

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS buffer (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def flush_to_delta():
    """Background task that flushes persistent SQLite buffer to Delta Lake."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM buffer ORDER BY id ASC LIMIT 1000") as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                continue
            
            row_ids = [row["id"] for row in rows]
            payloads = [json.loads(row["payload"]) for row in rows]
            
            print(f"[{datetime.now()}] Attempting to flush {len(payloads)} records from SQLite to Delta...")
            df = pd.DataFrame(payloads)
            
            try:
                # 1. Write to Delta Lake
                write_deltalake(TABLE_PATH, df, mode="append", partition_by="partition")
                
                # 2. DELETE from SQLite ONLY after successful Delta commit
                await db.execute(f"DELETE FROM buffer WHERE id IN ({','.join(map(str, row_ids))})")
                await db.commit()
                print(f"[{datetime.now()}] Successfully committed and cleared {len(payloads)} rows.")
            except Exception as e:
                print(f"[{datetime.now()}] FAILED to flush: {e}. Data remains in SQLite for retry.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure warehouse and DB exist
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    await init_db()
    
    # Start the flush worker
    flush_task = asyncio.create_task(flush_to_delta())
    yield
    # Cleanup
    flush_task.cancel()
    try:
        await flush_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

@app.post("/ingest")
async def ingest(request: Request):
    data = await request.json()
    records = data if isinstance(data, list) else [data]
    
    for r in records:
        if "created_at" not in r:
            r["created_at"] = datetime.now().isoformat()
        if "created_partition" not in r:
            r["created_partition"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    async with aiosqlite.connect(DB_PATH) as db:
        for r in records:
            await db.execute("INSERT INTO buffer (payload) VALUES (?)", (json.dumps(r),))
        await db.commit()
        
    return {"status": "accepted", "message": f"{len(records)} records persisted to WAL"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
