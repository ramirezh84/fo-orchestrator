import os
import asyncio
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="FO Demo App")

DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "appdb")
DB_USER = os.environ.get("DB_USER", "appuser")
DB_PASS = os.environ.get("DB_PASS", "")
REGION  = os.environ.get("REGION", os.environ.get("AWS_DEFAULT_REGION", "unknown"))


@app.get("/healthcheck")
async def healthcheck():
    return {
        "status": "healthy",
        "region": REGION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/deep-healthcheck")
async def deep_healthcheck():
    if not DB_HOST or DB_HOST == "placeholder":
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "db": "not configured",
                "region": REGION,
            },
        )

    try:
        conn = await asyncio.wait_for(
            asyncpg.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
            ),
            timeout=5.0,
        )
        row = await conn.fetchrow("SELECT version() AS ver, now() AS db_time")
        await conn.close()
        return {
            "status": "healthy",
            "db": "connected",
            "db_version": row["ver"],
            "db_time": str(row["db_time"]),
            "region": REGION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "db": "timeout",
                "region": REGION,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "db": "error",
                "error": str(exc),
                "region": REGION,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
