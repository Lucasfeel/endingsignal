import type { ContentCard, SubscriptionItem } from "../shared/types";

export const NAV_TABS = ["home", "webtoon", "novel", "ott", "my"] as const;
export type NavTab = (typeof NAV_TABS)[number];

export const SOURCE_OPTIONS = {
  webtoon: [
    { id: "naver_webtoon", label: "Naver" },
    { id: "kakaowebtoon", label: "Kakao" },
  ],
  novel: [
    { id: "naver_series", label: "Naver" },
    { id: "kakao_page", label: "KakaoPage" },
    { id: "ridi", label: "RIDI" },
  ],
  ott: [
    { id: "netflix", label: "Netflix" },
    { id: "tving", label: "TVING" },
    { id: "wavve", label: "wavve" },
    { id: "coupangplay", label: "Coupang Play" },
    { id: "disney_plus", label: "Disney+" },
    { id: "laftel", label: "Laftel" },
  ],
} as const;

export const SOURCE_BRAND_META: Record<string, { bg: string; logoColor?: string }> = {
  naver_webtoon: { bg: "#00DC64", logoColor: "#111111" },
  kakaowebtoon: { bg: "#FFD400", logoColor: "#111111" },
  naver_series: { bg: "#03C75A", logoColor: "#111111" },
  kakao_page: { bg: "#FEE102", logoColor: "#111111" },
  ridi: { bg: "#1E9EFF", logoColor: "#FFFFFF" },
  netflix: { bg: "#FFFFFF", logoColor: "#111111" },
  tving: { bg: "#FF143C", logoColor: "#FFFFFF" },
  wavve: { bg: "#1550F5", logoColor: "#FFFFFF" },
  coupangplay: { bg: "#FFFFFF", logoColor: "#111111" },
  disney_plus: { bg: "#01147C", logoColor: "#FFFFFF" },
  laftel: { bg: "#826CFF", logoColor: "#FFFFFF" },
};

export const SOURCE_LOGO_ASSETS: Record<string, string> = {
  naver_webtoon: "/static/source_logos/naver_webtoon.png",
  kakaowebtoon: "/static/source_logos/kakaowebtoon.jpg",
  naver_series: "/static/source_logos/naver_series.png",
  kakao_page: "/static/source_logos/kakao_page.jpeg",
  ridi: "/static/source_logos/ridi.jpeg",
  netflix: "/static/source_logos/netflix.jpeg",
  tving: "/static/source_logos/tving.png",
  wavve: "/static/source_logos/wavve.png",
  coupangplay: "/static/source_logos/coupangplay.png",
  disney_plus: "/static/source_logos/disney_plus.jpeg",
  laftel: "/static/source_logos/laftel.png",
};

export const WEBTOON_DAY_FILTER_OPTIONS = [
  { id: "all", label: "전체" },
  { id: "mon", label: "월" },
  { id: "tue", label: "화" },
  { id: "wed", label: "수" },
  { id: "thu", label: "목" },
  { id: "fri", label: "금" },
  { id: "sat", label: "토" },
  { id: "sun", label: "일" },
  { id: "daily", label: "매일" },
  { id: "hiatus", label: "휴재" },
  { id: "completed", label: "완결" },
] as const;

export const NOVEL_FILTER_OPTIONS = [
  { id: "all", label: "전체" },
  { id: "fantasy", label: "판타지" },
  { id: "hyeonpan", label: "현판" },
  { id: "romance", label: "로맨스" },
  { id: "romance_fantasy", label: "로판" },
  { id: "mystery", label: "미스터리" },
  { id: "light_novel", label: "라이트노벨" },
  { id: "wuxia", label: "무협" },
  { id: "bl", label: "BL" },
  { id: "completed", label: "완결" },
] as const;

export const OTT_FILTER_OPTIONS = [
  { id: "all", label: "전체" },
  { id: "drama", label: "드라마" },
  { id: "anime", label: "애니메이션" },
  { id: "variety", label: "예능" },
  { id: "docu", label: "다큐멘터리" },
  { id: "etc", label: "기타" },
  { id: "completed", label: "완결" },
] as const;

export function normalizeSourceId(sourceId: string) {
  const lower = String(sourceId || "").trim().toLowerCase();
  if (lower === "kakao_webtoon") return "kakaowebtoon";
  if (lower === "coupang_play") return "coupangplay";
  return lower;
}

export function getBasePathForTab(tab: NavTab) {
  if (tab === "home") return "/";
  if (tab === "my") return "/subscriptions";
  return `/browse/${tab}`;
}

export function isSearchPath(pathname: string) {
  return pathname === "/search";
}

export function isMyPagePath(pathname: string) {
  return pathname === "/mypage";
}

export function parseContentPath(pathname: string) {
  const match = pathname.match(/^\/content\/([^/]+)\/(.+)$/);
  if (!match) return null;
  return {
    source: decodeURIComponent(match[1]),
    contentId: decodeURIComponent(match[2]),
  };
}

export function getGridCountLabel(activeTab: NavTab, count: number) {
  if (activeTab === "home") return `현재 표시 중인 작품 ${count}개`;
  if (activeTab === "my" && count === 0) return "";
  if (activeTab === "my") return `현재 ${count}개 작품`;
  return `현재 표시 중인 작품 ${count}개`;
}

export function resolveCardTabId(content: Partial<ContentCard | SubscriptionItem>, fallback: NavTab) {
  const raw = String(content.content_type || fallback || "").trim().toLowerCase();
  if (raw === "webtoon" || raw === "novel" || raw === "ott") {
    return raw as NavTab;
  }
  return fallback;
}
