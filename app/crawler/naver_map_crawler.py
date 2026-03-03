"""
네이버 지도 방문자 리뷰 수집

place_id 조회: 공식 네이버 로컬 검색 API (link 필드에 place_id 포함)
리뷰 수집: place.map.naver.com 내부 API

Before 측정: 카페 1건씩 순차 처리, 소요시간 / 메모리 기록
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
import psutil

from app.core.config import settings
from app.core.database import get_cafes_collection, get_reviews_collection

logger = logging.getLogger(__name__)

NAVER_LOCAL_API = "https://openapi.naver.com/v1/search/local.json"
NAVER_REVIEW_API = "https://place.map.naver.com/place/v1/place/{place_id}/review/visitor"
REVIEWS_PER_CAFE = 10
REQUEST_DELAY = 0.5

NAVER_HEADERS = {
    "X-Naver-Client-Id": settings.naver_client_id,
    "X-Naver-Client-Secret": settings.naver_client_secret,
}

REVIEW_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://map.naver.com/",
}


@dataclass
class MapCrawlResult:
    total_cafes: int = 0
    total_fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed_cafes: list = field(default_factory=list)
    elapsed_sec: float = 0.0
    peak_memory_mb: float = 0.0

    @property
    def throughput(self) -> float:
        return self.total_fetched / self.elapsed_sec if self.elapsed_sec > 0 else 0


class NaverMapCrawler:
    def __init__(self, request_delay: float = REQUEST_DELAY):
        self.request_delay = request_delay

    async def _get_place_id(self, client: httpx.AsyncClient, cafe_name: str, district: str) -> str | None:
        """공식 네이버 로컬 검색 API → link URL에서 place_id 추출"""
        try:
            resp = await client.get(
                NAVER_LOCAL_API,
                params={"query": f"{cafe_name} {district}", "display": 1},
                headers=NAVER_HEADERS,
                timeout=10.0,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                # link 예시: https://map.naver.com/v5/entry/place/1234567890
                match = re.search(r"/place/(\d+)", items[0].get("link", ""))
                if match:
                    return match.group(1)
        except Exception as e:
            logger.debug(f"[{cafe_name}] place_id 검색 실패: {e}")
        return None

    async def _get_reviews(self, client: httpx.AsyncClient, place_id: str, cafe_name: str) -> list[dict]:
        try:
            resp = await client.get(
                NAVER_REVIEW_API.format(place_id=place_id),
                params={"reviewCount": REVIEWS_PER_CAFE, "lang": "ko"},
                headers=REVIEW_HEADERS,
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("result", {}).get("items", [])
            reviews = []
            for item in items:
                text = item.get("body", "") or item.get("contents", "")
                if text.strip():
                    reviews.append({
                        "source_id": str(item.get("id", "")),
                        "text": text.strip(),
                        "rating": item.get("rating"),
                        "author": item.get("writerNickname", ""),
                        "created_at": _parse_naver_ts(item.get("createdAt", "")),
                    })
            return reviews
        except Exception as e:
            logger.debug(f"[{cafe_name}] 리뷰 수집 실패 (place_id={place_id}): {e}")
        return []

    def _upsert_reviews(self, reviews: list[dict], cafe: dict, place_id: str) -> tuple[int, int]:
        col = get_reviews_collection()
        now = datetime.now(timezone.utc)
        inserted = updated = 0

        for review in reviews:
            source_id = f"naver_map_{place_id}_{review['source_id']}" if review["source_id"] else f"naver_map_{place_id}_{hash(review['text'])}"
            doc = {
                "cafe_id": cafe["kakao_id"],
                "cafe_name": cafe["name"],
                "naver_place_id": place_id,
                "source": "naver_map",
                "source_id": source_id,
                "text": review["text"],
                "rating": review.get("rating"),
                "author": review.get("author", ""),
                "created_at": review["created_at"],
                "updated_at": now,
                "is_processed": False,
            }
            result = col.update_one(
                {"cafe_id": doc["cafe_id"], "source_id": source_id},
                {
                    "$set": {k: v for k, v in doc.items() if k != "collected_at"},
                    "$setOnInsert": {"collected_at": now},
                },
                upsert=True,
            )
            if result.upserted_id:
                inserted += 1
            else:
                updated += 1

        # cafes 컬렉션에 naver_place_id 캐싱 (재크롤링 시 검색 생략)
        get_cafes_collection().update_one(
            {"kakao_id": cafe["kakao_id"]},
            {"$set": {"naver_place_id": place_id}},
        )
        return inserted, updated

    async def run(self, limit: int | None = None) -> MapCrawlResult:
        result = MapCrawlResult()
        proc = psutil.Process()
        started = time.perf_counter()
        peak_mem = proc.memory_info().rss

        cafes_col = get_cafes_collection()
        cafes = list(cafes_col.find({}, {"kakao_id": 1, "name": 1, "district": 1, "naver_place_id": 1}))
        if limit:
            cafes = cafes[:limit]
        result.total_cafes = len(cafes)

        async with httpx.AsyncClient(timeout=10.0) as client:
            for cafe in cafes:
                try:
                    place_id = cafe.get("naver_place_id")
                    if not place_id:
                        place_id = await self._get_place_id(client, cafe["name"], cafe.get("district", ""))
                        await asyncio.sleep(self.request_delay)

                    if not place_id:
                        result.failed_cafes.append(cafe["name"])
                        continue

                    reviews = await self._get_reviews(client, place_id, cafe["name"])
                    if reviews:
                        ins, upd = self._upsert_reviews(reviews, cafe, place_id)
                        result.total_fetched += len(reviews)
                        result.inserted += ins
                        result.updated += upd
                        logger.info(f"[{cafe['name']}] 리뷰 {len(reviews)}건 (신규 {ins} / 갱신 {upd})")
                    else:
                        result.failed_cafes.append(cafe["name"])

                    mem = proc.memory_info().rss
                    if mem > peak_mem:
                        peak_mem = mem

                    await asyncio.sleep(self.request_delay)

                except Exception as e:
                    logger.error(f"[{cafe['name']}] 처리 오류: {e}")
                    result.failed_cafes.append(cafe["name"])

        result.elapsed_sec = time.perf_counter() - started
        result.peak_memory_mb = peak_mem / 1024 / 1024

        logger.info(
            f"\n=== 네이버 지도 크롤링 완료 ===\n"
            f"  카페: {result.total_cafes}개\n"
            f"  수집 리뷰: {result.total_fetched}건\n"
            f"  신규: {result.inserted} / 갱신: {result.updated}\n"
            f"  실패: {len(result.failed_cafes)}개\n"
            f"  처리량: {result.throughput:.2f}건/초\n"
            f"  소요: {result.elapsed_sec:.1f}초\n"
            f"  최대 메모리: {result.peak_memory_mb:.1f}MB"
        )
        return result


def _parse_naver_ts(ts: str) -> datetime:
    """'2024-01-15T12:00:00' 또는 빈값 → datetime"""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    crawler = NaverMapCrawler()
    result = await crawler.run(limit=10)
    print(
        f"\n[Before 측정 - 네이버 지도]\n"
        f"  {result.total_fetched}건 / {result.elapsed_sec:.1f}초 "
        f"/ {result.throughput:.2f}건/초 / 메모리 {result.peak_memory_mb:.1f}MB"
    )


if __name__ == "__main__":
    asyncio.run(main())
