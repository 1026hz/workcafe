"""
APScheduler - 일일 자동 크롤링 스케줄러

Job 1 (새벽 3시): 카카오 로컬 API → 카페 목록 갱신 (전체 upsert)
Job 2 (새벽 4시): 네이버 블로그 API → 리뷰 포스팅 수집

FastAPI lifespan에서 시작/종료 관리
"""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.database import get_batch_logs_collection

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")


async def _run_kakao_crawl():
    from app.crawler.kakao_crawler import KakaoCrawler

    job_name = "kakao_cafe_crawl"
    started_at = datetime.now(timezone.utc)
    logger.info(f"[{job_name}] 시작")

    try:
        result = await KakaoCrawler().run()
        status = "success"
        summary = {
            "total_fetched": result.total_fetched,
            "inserted": result.inserted,
            "updated": result.updated,
            "elapsed_sec": result.elapsed_sec,
        }
    except Exception as e:
        logger.error(f"[{job_name}] 실패: {e}")
        status = "failed"
        summary = {"error": str(e)}

    get_batch_logs_collection().insert_one({
        "job_name": job_name,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc),
        "status": status,
        **summary,
    })


async def _run_naver_blog_crawl():
    from app.crawler.naver_blog_crawler import NaverBlogCrawler

    job_name = "naver_blog_crawl"
    started_at = datetime.now(timezone.utc)
    logger.info(f"[{job_name}] 시작")

    try:
        result = await NaverBlogCrawler().run()
        status = "success"
        summary = {
            "total_cafes": result.total_cafes,
            "total_fetched": result.total_fetched,
            "inserted": result.inserted,
            "updated": result.updated,
            "elapsed_sec": result.elapsed_sec,
            "peak_memory_mb": result.peak_memory_mb,
        }
    except Exception as e:
        logger.error(f"[{job_name}] 실패: {e}")
        status = "failed"
        summary = {"error": str(e)}

    get_batch_logs_collection().insert_one({
        "job_name": job_name,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc),
        "status": status,
        **summary,
    })


def start_scheduler():
    # 매일 03:00 카카오 카페 목록 갱신
    scheduler.add_job(
        _run_kakao_crawl,
        trigger=CronTrigger(hour=3, minute=0),
        id="kakao_cafe_crawl",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # 매일 04:00 네이버 블로그 리뷰 수집
    scheduler.add_job(
        _run_naver_blog_crawl,
        trigger=CronTrigger(hour=4, minute=0),
        id="naver_blog_crawl",
        replace_existing=True,
        misfire_grace_time=300,
    )

    scheduler.start()
    logger.info("스케줄러 시작: 카카오(03:00) / 네이버 블로그(04:00)")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("스케줄러 종료")
