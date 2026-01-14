import asyncio

import config
from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


async def main():
    crawler = KakaoWebtoonCrawler()
    (
        ongoing_today,
        hiatus_today,
        finished_today,
        all_content_today,
        fetch_meta,
    ) = await crawler.fetch_all_data()

    print("\n[KakaoWebtoon] Profile lookup summary")
    print(
        "  candidates="
        f"{fetch_meta.get('completed_candidate_total')} "
        "budget="
        f"{config.KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET} "
        "lookups="
        f"{fetch_meta.get('profile_lookup_total')} "
        "ok="
        f"{fetch_meta.get('profile_lookup_ok')} "
        "failed="
        f"{fetch_meta.get('profile_lookup_failed')} "
        "skipped_due_to_budget="
        f"{fetch_meta.get('lookup_skipped_due_to_budget')}"
    )

    completed_candidates = [
        entry
        for entry in all_content_today.values()
        if entry.get("kakao_completed_candidate")
    ]
    print(f"  completed_candidates_sample={len(completed_candidates)}")
    for entry in completed_candidates[:5]:
        checked_at = entry.get("kakao_profile_status_checked_at")
        print(
            f"  - {entry['title']} ({entry['content_id']}) "
            f"profile_status={entry.get('kakao_profile_status')} "
            f"lookup_performed={bool(checked_at)} "
            f"unverified_candidate={entry.get('kakao_unverified_completed_candidate')}"
        )


if __name__ == "__main__":
    asyncio.run(main())
