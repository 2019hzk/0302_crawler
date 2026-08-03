#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
平台爬虫管理器 — 通过直接导入 MediaCrawler 模块执行多平台爬取。

配置通过修改 MediaCrawler 的 config 模块变量传递，替代原来的子进程模式。
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from loguru import logger

from app.config import (
    DB_DIALECT, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
    DEFAULT_MAX_NOTES, DEFAULT_LOGIN_TYPE, SUPPORTED_PLATFORMS,
    CRAWLER_HEADLESS, CRAWLER_ENABLE_CDP_MODE, CRAWLER_DISABLE_SSL_VERIFY,
)

MEDIACRAWLER_PATH = Path(__file__).resolve().parent.parent.parent / "MediaCrawler"


class PlatformCrawler:
    """平台爬虫管理器 — 直接调用 MediaCrawler 模块"""

    def __init__(self):
        self.crawl_stats: Dict = {}
        logger.info("初始化平台爬虫管理器（直接调用模式）")

    def _init_common_config(
        self, keywords: List[str], max_notes: int, login_type: str, platform: str = "",
    ) -> None:
        """注入 MediaCrawler 公共配置"""
        is_postgresql = (DB_DIALECT or "mysql").lower() in ("postgresql", "postgres")
        save_data_option = "postgresql" if is_postgresql else "db"

        prev_cwd = os.getcwd()   # 记住当前目录
        os.chdir(str(MEDIACRAWLER_PATH))  # 切到 MediaCrawler 目录
        sys.path.insert(0, str(MEDIACRAWLER_PATH))
        try:
            import config as mc_config
            if platform == "wb":
                login_type = "qrcode"
                mc_config.CDP_CONNECT_EXISTING = False

            mc_config.KEYWORDS = ",".join(keywords)
            mc_config.LOGIN_TYPE = login_type
            mc_config.CRAWLER_TYPE = "search"
            mc_config.CRAWLER_MAX_NOTES_COUNT = max_notes
            mc_config.HEADLESS = False if platform == "wb" else CRAWLER_HEADLESS
            mc_config.ENABLE_CDP_MODE = True if platform == "wb" else CRAWLER_ENABLE_CDP_MODE
            mc_config.DISABLE_SSL_VERIFY = CRAWLER_DISABLE_SSL_VERIFY

            mc_config.ENABLE_GET_COMMENTS = True
            mc_config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = 20
            mc_config.SAVE_DATA_OPTION = save_data_option

            from config import db_config as mc_db_config
            mc_db_config.MYSQL_DB_HOST = DB_HOST
            mc_db_config.MYSQL_DB_PORT = int(DB_PORT)
            mc_db_config.MYSQL_DB_USER = DB_USER
            mc_db_config.MYSQL_DB_PASSWORD = DB_PASSWORD
            mc_db_config.MYSQL_DB_NAME = DB_NAME
            mc_db_config.mysql_db_config.update({
                "host": DB_HOST,
                "port": int(DB_PORT),
                "user": DB_USER,
                "password": DB_PASSWORD,
                "db_name": DB_NAME,
            })
        finally:
            os.chdir(prev_cwd)  #  无论成功或异常，都切回原目录

    async def _run_crawler_async(
        self, platform: str, keywords: List[str],
        login_type: str, max_notes: int,
    ) -> Dict:
        """异步执行单个平台的爬取"""
        logger.info(
            f"开始爬取平台: {platform}, 关键词: {len(keywords)} 个, "
            f"登录方式: {login_type}"
        )

        start_time = datetime.now()

        prev_cwd = os.getcwd()
        os.chdir(str(MEDIACRAWLER_PATH))
        sys.path.insert(0, str(MEDIACRAWLER_PATH))

        try:
            import config as mc_config
            mc_config.PLATFORM = platform

            from main import CrawlerFactory
            crawler = CrawlerFactory.create_crawler(platform=platform)
            await crawler.start()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            crawl_stats = {
                "platform": platform,
                "keywords_count": len(keywords),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "success": True,
                "notes_count": 0,
                "comments_count": 0,
                "errors_count": 0,
            }

            self.crawl_stats[platform] = crawl_stats
            logger.info(f"{platform} 爬取完成，耗时: {duration:.1f}秒")
            return crawl_stats

        except Exception as e:
            logger.exception(f"{platform} 爬取异常: {e}")
            return {"success": False, "error": str(e), "platform": platform}
        finally:
            os.chdir(prev_cwd)

    def run_crawler(
        self, platform: str, keywords: List[str],
        login_type: str = None, max_notes: int = None,
    ) -> Dict:
        """运行单个平台的爬虫（同步入口）"""
        if platform not in SUPPORTED_PLATFORMS:
            raise ValueError(f"不支持的平台: {platform}")
        if not keywords:
            raise ValueError("关键词列表不能为空")

        if login_type is None:
            login_type = DEFAULT_LOGIN_TYPE
        if max_notes is None:
            max_notes = DEFAULT_MAX_NOTES

        self._init_common_config(keywords, max_notes, login_type, platform)
        if platform == "wb":
            login_type = "qrcode"
        return asyncio.run(
            self._run_crawler_async(platform, keywords, login_type, max_notes)
        )

    async def run_multi_platform_crawl_by_keywords(
        self, keywords: List[str], platforms: List[str],
        login_type: str = None, max_notes_per_keyword: int = None,
    ) -> Dict:
        """基于关键词的多平台爬取"""
        if login_type is None:
            login_type = DEFAULT_LOGIN_TYPE
        if max_notes_per_keyword is None:
            max_notes_per_keyword = DEFAULT_MAX_NOTES

        logger.info(
            f"开始全平台关键词爬取: {len(keywords)} 个关键词 × {len(platforms)} 个平台 "
            f"= {len(keywords) * len(platforms)} 个任务"
        )

        total_stats = {
            "total_keywords": len(keywords),
            "total_platforms": len(platforms),
            "total_tasks": len(keywords) * len(platforms),
            "successful_tasks": 0,
            "failed_tasks": 0,
            "total_notes": 0,
            "total_comments": 0,
            "keyword_results": {},
            "platform_summary": {},
        }

        for platform in platforms:
            total_stats["platform_summary"][platform] = {
                "successful_keywords": 0, "failed_keywords": 0,
                "total_notes": 0, "total_comments": 0,
            }

            if platform == "wb":
                run_login_type = "qrcode"
            else:
                run_login_type = login_type
            self._init_common_config(keywords, max_notes_per_keyword, run_login_type, platform)

            logger.info(f"在 {platform} 平台爬取所有关键词")

            try:
                result = await self._run_crawler_async(
                    platform, keywords, run_login_type, max_notes_per_keyword,
                )

                if result.get("success"):
                    total_stats["successful_tasks"] += len(keywords)
                    total_stats["platform_summary"][platform]["successful_keywords"] = len(keywords)

                    notes_count = result.get("notes_count", 0)
                    comments_count = result.get("comments_count", 0)
                    total_stats["total_notes"] += notes_count
                    total_stats["total_comments"] += comments_count
                    total_stats["platform_summary"][platform]["total_notes"] = notes_count
                    total_stats["platform_summary"][platform]["total_comments"] = comments_count

                    for keyword in keywords:
                        total_stats["keyword_results"].setdefault(keyword, {})[platform] = result

                    logger.info(f"  {platform} 爬取成功")
                else:
                    total_stats["failed_tasks"] += len(keywords)
                    total_stats["platform_summary"][platform]["failed_keywords"] = len(keywords)

                    for keyword in keywords:
                        total_stats["keyword_results"].setdefault(keyword, {})[platform] = result

                    logger.error(f"  {platform} 失败: {result.get('error', '未知错误')}")

            except Exception as e:
                total_stats["failed_tasks"] += len(keywords)
                total_stats["platform_summary"][platform]["failed_keywords"] = len(keywords)
                error_result = {"success": False, "error": str(e)}
                for keyword in keywords:
                    total_stats["keyword_results"].setdefault(keyword, {})[platform] = error_result
                logger.error(f"  {platform} 异常: {e}")

        success_rate = (
            total_stats['successful_tasks'] / total_stats['total_tasks'] * 100
            if total_stats['total_tasks'] else 0
        )
        logger.info(
            f"全平台爬取完成: {total_stats['successful_tasks']}/{total_stats['total_tasks']} "
            f"成功 ({success_rate:.1f}%)"
        )

        return total_stats

    def get_crawl_statistics(self) -> Dict:
        """获取爬取统计信息"""
        return {
            "platforms_crawled": list(self.crawl_stats.keys()),
            "total_platforms": len(self.crawl_stats),
            "detailed_stats": self.crawl_stats,
        }
