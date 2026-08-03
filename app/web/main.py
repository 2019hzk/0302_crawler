# -*- coding: utf-8 -*-
"""
FastAPI Web 服务 — 爬虫前端 API 入口。

启动方式:
    uv run python -m app.web.main
    uv run uvicorn app.web.main:app --host 0.0.0.0 --port 8080
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[web] 正在启动 Web 服务...")
    from app.web.routers.crawl import init_task_manager
    init_task_manager()
    logger.info("[web] TaskManager 已初始化")
    yield
    from app.db.session import dispose_engine
    await dispose_engine()
    logger.info("[web] Web 服务已关闭")


app = FastAPI(
    title="Sentinel Crawler",
    description="多平台社交媒体爬虫控制台",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====================== REST 路由 ======================

from app.web.routers import crawl, topics, news, platforms, websocket

app.include_router(crawl.router)
app.include_router(topics.router)
app.include_router(news.router)
app.include_router(platforms.router)
app.include_router(websocket.router)


# ====================== 健康检查 ======================

@app.get("/api/health")
async def health():
    try:
        from sqlalchemy import text
        from app.db.session import get_session
        async with get_session() as session:
            await session.execute(text("SELECT 1"))
        db_ok = True
    except Exception as e:
        db_ok = False
    return {"status": "ok" if db_ok else "degraded", "db_connected": db_ok}


@app.get("/")
async def root():
    return {
        "message": "爬虫 API 服务已启动",
        "docs": "/docs",
        "frontend": "请运行 cd frontend && npm run dev 启动前端开发服务器",
    }


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("WEB_HOST", "0.0.0.0")
    uvicorn.run("app.web.main:app", host=host, port=9000, reload=True)
