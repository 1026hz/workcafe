"""
Playwright로 네이버 지도 방문자 리뷰 수집

흐름:
  1. 네이버 검색(장소)으로 카페 검색 → place_id 추출
  2. place.map.naver.com/place/{place_id}/review/visitor 접근
  3. 방문자 리뷰 텍스트, 별점, 날짜 수집
  4. reviews 컬렉션에 upsert

Before 측정: 카페 1건씩 순차 처리, 총 소요시간 / 메모리 기록
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import quote

import psutil
from playwright.async_api import async_playwright, Browser, BrowserContext

from app.core.database import get_cafes_collection, get_reviews_collection

logger = logging.getLogger(__name__)

NAVER_SEARCH_URL = "https://search.naver.com/search.naver?where=place&query={query}"
NAVER_PLACE_REVIEW_URL = "https://place.map.naver.com/place/{place_id}/review/visitor"
REVIEWS_PER_CAFE = 10       # 카페당 최대 리뷰 수
PAGE_TIMEOUT = 15_000       # ms
REQUEST_DELAY = 1.5         # 초 (봇 감지 방지)


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


def _parse_naver_date(date_str: str) -> datetime:
    """'2024.01.15' or '3일 전' 등 → datetime"""
    try:
        return datetime.strptime(date_str.strip(), "%Y.%m.%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


class NaverMapCrawler:
    def __init__(self, headless: bool = True, request_delay: float = REQUEST_DELAY):
        self.headless = headless
        self.request_delay = request_delay

    async def _get_place_id(self, context: BrowserContext, cafe_name: str, district: str) -> str | None:
        """네이버 장소 검색으로 place_id 추출"""
        query = quote(f"{cafe_name} {district}")
        url = NAVER_SEARCH_URL.format(query=query)

        page = await context.new_page()
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_selector(".place_section", timeout=PAGE_TIMEOUT)

            # 첫 번째 검색 결과에서 place_id 추출
            first = await page.query_selector("a[href*='/place/']")
            if not first:
                return None

            href = await first.get_attribute("href")
            match = re.search(r"/place/(\d+)", href or "")
            return match.group(1) if match else None
        except Exception as e:
            logger.debug(f"[{cafe_name}] place_id 추출 실패: {e}")
            return None
        finally:
            await page.close()

    async def _get_reviews(self, context: BrowserContext, place_id: str, cafe_name: str) -> list[dict]:
        """방문자 리뷰 페이지에서 리뷰 수집"""
        url = NAVER_PLACE_REVIEW_URL.format(place_id=place_id)
        page = await context.new_page()
        reviews = []

        try:
            await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_selector(".pui__vn15t2", timeout=PAGE_TIMEOUT)

            items = await page.query_selector_all(".pui__vn15t2")
            for item in items[:REVIEWS_PER_CAFE]:
                try:
                    text_el = await item.query_selector(".pui__vn15t2 span.pui__xSSux")
                    date_el = await item.query_selector("time, .pui__gfuPef")
                    rating_el = await item.query_selector(".pui__eNSmKN em")

                    text = await text_el.inner_text() if text_el else ""
                    date_str = await date_el.inner_text() if date_el else ""
                    rating_str = await rating_el.inner_text() if rating_el else "0"

                    if text.strip():
                        reviews.append({
                            "text": text.strip(),
                            "created_at": _parse_naver_date(date_str),
                            "rating": float(rating_str) if rating_str.replace(".", "").isdigit() else None,
                        })
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"[{cafe_name}] 리뷰 수집 실패 (place_id={place_id}): {e}")
        finally:
            await page.close()

        return reviews

    def _upsert_reviews(self, reviews: list[dict], cafe: dict, place_id: str) -> tuple[int, int]:
        col = get_reviews_collection()
        now = datetime.now(timezone.utc)
        inserted = updated = 0

        for idx, review in enumerate(reviews):
            source_id = f"naver_map_{place_id}_{idx}"  # 리뷰 고유 ID가 없으므로 인덱스 사용
            doc = {
                "cafe_id": cafe["kakao_id"],
                "cafe_name": cafe["name"],
                "naver_place_id": place_id,
                "source": "naver_map",
                "source_id": source_id,
                "text": review["text"],
                "rating": review.get("rating"),
                "created_at": review["created_at"],
                "collected_at": now,
                "updated_at": now,
                "is_processed": False,
            }
            result = col.update_one(
                {"cafe_id": doc["cafe_id"], "source_id": source_id},
                {"$set": doc, "$setOnInsert": {"collected_at": now}},
                upsert=True,
            )
            if result.upserted_id:
                inserted += 1
            else:
                updated += 1

        # cafes 컬렉션에 naver_place_id 저장 (이후 재크롤링 시 검색 생략)
        get_cafes_collection().update_one(
            {"kakao_id": cafe["kakao_id"]},
            {"$set": {"naver_place_id": place_id}},
        )
        return inserted, updated

    async def run(self, limit: int | None = None) -> MapCrawlResult:
        """
        전체 카페 순차 처리 (Before 측정)
        limit: 테스트 시 카페 수 제한
        """
        result = MapCrawlResult()
        proc = psutil.Process()
        started = time.perf_counter()
        peak_mem = proc.memory_info().rss

        cafes_col = get_cafes_collection()
        cafes = list(cafes_col.find({}, {"kakao_id": 1, "name": 1, "district": 1, "naver_place_id": 1}))
        if limit:
            cafes = cafes[:limit]
        result.total_cafes = len(cafes)

        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(headless=self.headless)
            context: BrowserContext = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="ko-KR",
            )

            for cafe in cafes:
                try:
                    # 이미 place_id 있으면 검색 생략 (증분 처리 준비)
                    place_id = cafe.get("naver_place_id")
                    if not place_id:
                        place_id = await self._get_place_id(context, cafe["name"], cafe.get("district", ""))
                        await asyncio.sleep(self.request_delay)

                    if not place_id:
                        logger.debug(f"[{cafe['name']}] place_id 없음, 스킵")
                        result.failed_cafes.append(cafe["name"])
                        continue

                    reviews = await self._get_reviews(context, place_id, cafe["name"])
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

            await context.close()
            await browser.close()

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


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    # 테스트: 카페 10개만
    crawler = NaverMapCrawler(headless=True)
    result = await crawler.run(limit=10)
    print(
        f"\n[Before 측정 - 네이버 지도]\n"
        f"  {result.total_fetched}건 / {result.elapsed_sec:.1f}초 "
        f"/ {result.throughput:.2f}건/초 / 메모리 {result.peak_memory_mb:.1f}MB"
    )


if __name__ == "__main__":
    asyncio.run(main())
