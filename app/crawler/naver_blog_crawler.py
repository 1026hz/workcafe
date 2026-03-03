"""
네이버 블로그 검색 API로 카페 리뷰 포스팅 수집

- 카페명 + 구 이름으로 블로그 검색
- reviews 컬렉션에 upsert (source_id = 블로그 포스팅 URL)
- Before 측정: 순차 처리 소요시간 / 처리량 기록
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
import psutil

from app.core.config import settings
from app.core.database import get_cafes_collection, get_reviews_collection

logger = logging.getLogger(__name__)

NAVER_BLOG_URL = "https://openapi.naver.com/v1/search/blog.json"
POSTS_PER_CAFE = 5       # 카페당 수집할 블로그 포스팅 수
REQUEST_DELAY = 0.1      # 초 (네이버 API rate limit 방지)


@dataclass
class BlogCrawlResult:
    total_cafes: int = 0
    total_fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed_cafes: list = field(default_factory=list)
    elapsed_sec: float = 0.0
    peak_memory_mb: float = 0.0

    @property
    def throughput(self) -> float:
        """초당 처리 건수"""
        return self.total_fetched / self.elapsed_sec if self.elapsed_sec > 0 else 0


def _parse_postdate(postdate: str) -> datetime:
    """'20240101' → datetime"""
    try:
        return datetime.strptime(postdate, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def _strip_html(text: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", text)


class NaverBlogCrawler:
    def __init__(self, request_delay: float = REQUEST_DELAY):
        self.headers = {
            "X-Naver-Client-Id": settings.naver_client_id,
            "X-Naver-Client-Secret": settings.naver_client_secret,
        }
        self.request_delay = request_delay

    async def _fetch_posts(
        self, client: httpx.AsyncClient, cafe_name: str, district: str
    ) -> list[dict]:
        query = f"{cafe_name} {district} 카페"
        params = {
            "query": query,
            "display": POSTS_PER_CAFE,
            "sort": "date",
        }
        try:
            resp = await client.get(NAVER_BLOG_URL, params=params, headers=self.headers)
            resp.raise_for_status()
            return resp.json().get("items", [])
        except httpx.HTTPStatusError as e:
            logger.warning(f"[{cafe_name}] 블로그 API 오류: {e.response.status_code}")
            return []
        except Exception as e:
            logger.warning(f"[{cafe_name}] 블로그 API 실패: {e}")
            return []

    def _build_review_doc(self, item: dict, cafe: dict, now: datetime) -> dict:
        return {
            "cafe_id": cafe["kakao_id"],
            "cafe_name": cafe["name"],
            "source": "naver_blog",
            "source_id": item["link"],          # URL = 고유 ID
            "title": _strip_html(item.get("title", "")),
            "text": _strip_html(item.get("description", "")),
            "author": item.get("bloggername", ""),
            "author_url": item.get("bloggerlink", ""),
            "created_at": _parse_postdate(item.get("postdate", "")),
            "collected_at": now,
            "updated_at": now,
            "is_processed": False,              # Day 3 LangGraph 처리 전
        }

    def _upsert_reviews(self, reviews: list[dict]) -> tuple[int, int]:
        col = get_reviews_collection()
        inserted = updated = 0
        for review in reviews:
            result = col.update_one(
                {"cafe_id": review["cafe_id"], "source_id": review["source_id"]},
                {
                    "$set": review,
                    "$setOnInsert": {"collected_at": review["collected_at"]},
                },
                upsert=True,
            )
            if result.upserted_id:
                inserted += 1
            else:
                updated += 1
        return inserted, updated

    async def run(self, limit: int | None = None) -> BlogCrawlResult:
        """
        전체 카페에 대해 순차적으로 블로그 포스팅 수집 (Before 측정용)
        limit: 테스트 시 카페 수 제한
        """
        result = BlogCrawlResult()
        proc = psutil.Process()
        started = time.perf_counter()
        peak_mem = proc.memory_info().rss

        cafes_col = get_cafes_collection()
        cafes = list(cafes_col.find({}, {"kakao_id": 1, "name": 1, "district": 1}))
        if limit:
            cafes = cafes[:limit]
        result.total_cafes = len(cafes)

        async with httpx.AsyncClient(timeout=10.0) as client:
            for cafe in cafes:
                now = datetime.now(timezone.utc)
                posts = await self._fetch_posts(client, cafe["name"], cafe.get("district", ""))
                if not posts:
                    result.failed_cafes.append(cafe["name"])
                    await asyncio.sleep(self.request_delay)
                    continue

                reviews = [self._build_review_doc(p, cafe, now) for p in posts]
                ins, upd = self._upsert_reviews(reviews)

                result.total_fetched += len(reviews)
                result.inserted += ins
                result.updated += upd

                # 메모리 최고점 추적
                mem = proc.memory_info().rss
                if mem > peak_mem:
                    peak_mem = mem

                await asyncio.sleep(self.request_delay)

        result.elapsed_sec = time.perf_counter() - started
        result.peak_memory_mb = peak_mem / 1024 / 1024

        logger.info(
            f"\n=== 네이버 블로그 크롤링 완료 ===\n"
            f"  카페 수: {result.total_cafes}\n"
            f"  수집 포스팅: {result.total_fetched}건\n"
            f"  신규: {result.inserted}건 / 갱신: {result.updated}건\n"
            f"  처리량: {result.throughput:.1f}건/초\n"
            f"  소요: {result.elapsed_sec:.1f}초\n"
            f"  최대 메모리: {result.peak_memory_mb:.1f}MB"
        )
        return result


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    crawler = NaverBlogCrawler()
    result = await crawler.run()
    print(
        f"\n[Before 측정]\n"
        f"  총 {result.total_fetched}건 / {result.elapsed_sec:.1f}초 "
        f"/ {result.throughput:.1f}건/초 / 메모리 {result.peak_memory_mb:.1f}MB"
    )


if __name__ == "__main__":
    asyncio.run(main())
