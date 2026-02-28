"""
카카오 로컬 API로 서울 카페 목록 수집

- 카테고리 코드 CE7 (카페)
- 서울 25개 구 중심 좌표 기반 검색 (반경 2km)
- kakao_id로 upsert → 멱등성 보장 (증분 크롤링 기반)
- Before/After 지표: inserted / updated 건수 분리 집계
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from app.core.config import settings
from app.core.database import get_cafes_collection

logger = logging.getLogger(__name__)

KAKAO_CATEGORY_URL = "https://dapi.kakao.com/v2/local/search/category.json"
CAFE_CATEGORY_CODE = "CE7"
MAX_PAGE = 45       # 카카오 API 최대 페이지
PAGE_SIZE = 15      # 카카오 API 고정값
SEARCH_RADIUS = 2000  # 미터

# 서울 25개 구 중심 좌표 (경도 x, 위도 y)
SEOUL_DISTRICTS = [
    {"name": "강남구",  "x": 127.0495, "y": 37.5172},
    {"name": "강동구",  "x": 127.1238, "y": 37.5301},
    {"name": "강북구",  "x": 127.0256, "y": 37.6396},
    {"name": "강서구",  "x": 126.8495, "y": 37.5509},
    {"name": "관악구",  "x": 126.9515, "y": 37.4784},
    {"name": "광진구",  "x": 127.0823, "y": 37.5385},
    {"name": "구로구",  "x": 126.8876, "y": 37.4954},
    {"name": "금천구",  "x": 126.8956, "y": 37.4569},
    {"name": "노원구",  "x": 127.0568, "y": 37.6541},
    {"name": "도봉구",  "x": 127.0471, "y": 37.6688},
    {"name": "동대문구", "x": 127.0401, "y": 37.5744},
    {"name": "동작구",  "x": 126.9397, "y": 37.5124},
    {"name": "마포구",  "x": 126.9024, "y": 37.5637},
    {"name": "서대문구", "x": 126.9368, "y": 37.5791},
    {"name": "서초구",  "x": 127.0325, "y": 37.4836},
    {"name": "성동구",  "x": 127.0369, "y": 37.5636},
    {"name": "성북구",  "x": 127.0176, "y": 37.5894},
    {"name": "송파구",  "x": 127.1067, "y": 37.5145},
    {"name": "양천구",  "x": 126.8687, "y": 37.5170},
    {"name": "영등포구", "x": 126.8963, "y": 37.5264},
    {"name": "용산구",  "x": 126.9644, "y": 37.5324},
    {"name": "은평구",  "x": 126.9228, "y": 37.6027},
    {"name": "종로구",  "x": 126.9784, "y": 37.5730},
    {"name": "중구",   "x": 126.9979, "y": 37.5638},
    {"name": "중랑구",  "x": 127.0927, "y": 37.6063},
]


@dataclass
class CrawlResult:
    total_fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed_districts: list = field(default_factory=list)
    elapsed_sec: float = 0.0

    @property
    def total_saved(self) -> int:
        return self.inserted + self.updated


def _build_cafe_doc(place: dict, district_name: str, now: datetime) -> dict:
    return {
        "kakao_id": place["id"],
        "name": place["place_name"],
        "category_code": CAFE_CATEGORY_CODE,
        "category_name": place.get("category_name", ""),
        "address": place.get("address_name", ""),
        "road_address": place.get("road_address_name", ""),
        "phone": place.get("phone", ""),
        "x": float(place["x"]),
        "y": float(place["y"]),
        "place_url": place.get("place_url", ""),
        "district": district_name,
        "source": "kakao",
        "updated_at": now,
    }


class KakaoCrawler:
    def __init__(self, request_delay: float = 0.3):
        self.headers = {"Authorization": f"KakaoAK {settings.kakao_api_key}"}
        self.request_delay = request_delay  # 초당 요청 제한 방지

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        x: float,
        y: float,
        page: int,
    ) -> tuple[list[dict], bool]:
        """API 1페이지 호출 → (결과 목록, is_end)"""
        params = {
            "category_group_code": CAFE_CATEGORY_CODE,
            "x": x,
            "y": y,
            "radius": SEARCH_RADIUS,
            "page": page,
            "size": PAGE_SIZE,
            "sort": "distance",
        }
        resp = await client.get(KAKAO_CATEGORY_URL, params=params, headers=self.headers)
        resp.raise_for_status()
        data = resp.json()
        return data["documents"], data["meta"]["is_end"]

    async def _crawl_district(
        self,
        client: httpx.AsyncClient,
        district: dict,
    ) -> list[dict]:
        """한 구의 카페 전체 수집 (페이지네이션)"""
        name = district["name"]
        x, y = district["x"], district["y"]
        now = datetime.now(timezone.utc)
        places = []

        for page in range(1, MAX_PAGE + 1):
            try:
                docs, is_end = await self._fetch_page(client, x, y, page)
                places.extend(docs)
                logger.debug(f"{name} p{page}: {len(docs)}건 (누적 {len(places)}건)")
                if is_end:
                    break
                await asyncio.sleep(self.request_delay)
            except httpx.HTTPStatusError as e:
                logger.warning(f"{name} p{page} 요청 실패: {e.response.status_code}")
                break
            except Exception as e:
                logger.warning(f"{name} p{page} 오류: {e}")
                break

        # 중복 kakao_id 제거 (동일 장소가 여러 구 검색에 걸칠 수 있음)
        seen = {}
        for p in places:
            if p["id"] not in seen:
                seen[p["id"]] = _build_cafe_doc(p, name, now)

        logger.info(f"{name}: {len(seen)}개 수집")
        return list(seen.values())

    def _upsert_cafes(self, cafes: list[dict]) -> tuple[int, int]:
        """MongoDB upsert. 반환: (inserted, updated)"""
        col = get_cafes_collection()
        inserted = updated = 0

        for cafe in cafes:
            kakao_id = cafe["kakao_id"]
            now = cafe["updated_at"]
            result = col.update_one(
                {"kakao_id": kakao_id},
                {
                    "$set": cafe,
                    "$setOnInsert": {"collected_at": now},  # 최초 수집 시각은 덮어쓰지 않음
                },
                upsert=True,
            )
            if result.upserted_id is not None:
                inserted += 1
            else:
                updated += 1

        return inserted, updated

    async def run(self) -> CrawlResult:
        result = CrawlResult()
        started = time.perf_counter()

        async with httpx.AsyncClient(timeout=10.0) as client:
            for district in SEOUL_DISTRICTS:
                try:
                    cafes = await self._crawl_district(client, district)
                    result.total_fetched += len(cafes)

                    ins, upd = self._upsert_cafes(cafes)
                    result.inserted += ins
                    result.updated += upd

                    logger.info(
                        f"[{district['name']}] 저장 완료 → "
                        f"신규 {ins}건 / 갱신 {upd}건"
                    )
                except Exception as e:
                    logger.error(f"[{district['name']}] 처리 실패: {e}")
                    result.failed_districts.append(district["name"])

                await asyncio.sleep(self.request_delay)

        result.elapsed_sec = time.perf_counter() - started
        logger.info(
            f"\n=== 크롤링 완료 ===\n"
            f"  수집: {result.total_fetched}건\n"
            f"  신규: {result.inserted}건\n"
            f"  갱신: {result.updated}건\n"
            f"  실패 구: {result.failed_districts or '없음'}\n"
            f"  소요: {result.elapsed_sec:.1f}초"
        )
        return result


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    crawler = KakaoCrawler()
    result = await crawler.run()
    print(f"\n완료: 신규 {result.inserted}건 / 갱신 {result.updated}건 / {result.elapsed_sec:.1f}초")


if __name__ == "__main__":
    asyncio.run(main())
