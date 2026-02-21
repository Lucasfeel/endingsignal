# Source Chip Brand Assets

This document tracks the source-of-truth references and the brand chip values used in `static/app.js` (`SOURCE_BRAND_META` and `SOURCE_LOGO_ASSETS`).

## References

- NAVER brand guide: <https://www.navercorp.com/company/brandGuide>
- Netflix logos: <https://brand.netflix.com/en/assets/logos/>
- Disney+ press logos: <https://press.disneyplus.com/about/logos>
- Coupang media assets guidelines: <https://news.coupang.com/media-assets-brand-guidelines-kr/>
- Brandfetch:
  - <https://brandfetch.com/netflix.com>
  - <https://brandfetch.com/tving.com>
  - <https://brandfetch.com/wavve.com>
  - <https://brandfetch.com/disneyplus.com>
  - <https://brandfetch.com/laftel.net>
  - <https://brandfetch.com/ridibooks.com>
  - <https://brandfetch.com/kakaopage.com>

## Brand Values In Code

| Source ID | Background | Border | Foreground (`--chip-fg`) | Asset |
| --- | --- | --- | --- | --- |
| `naver_series` | `#03C75A` | `rgba(0,0,0,0.06)` | `#111111` | `static/source_logos/naver_series.svg` |
| `kakao_page` | `#FEE102` | `rgba(0,0,0,0.08)` | `#111111` | `static/source_logos/kakao_page.svg` |
| `munpia` | `#2F80FF` | `rgba(255,255,255,0.25)` | `#FFFFFF` | `static/source_logos/munpia.svg` |
| `ridi` | `#1E9EFF` | `rgba(255,255,255,0.25)` | `#FFFFFF` | `static/source_logos/ridi.svg` |
| `netflix` | `#FFFFFF` | `#E5E8EB` | _not forced_ | `static/source_logos/netflix.png` |
| `tving` | `#000000` | `rgba(255,255,255,0.12)` | `#FF143C` | `static/source_logos/tving.svg` |
| `wavve` | `linear-gradient(135deg, #5DD0FF 0%, #B4EFFF 100%)` | `rgba(255,255,255,0.25)` | `#FFFFFF` | _kept as existing inline fallback_ |
| `coupangplay` | `#FFFFFF` | `#E5E8EB` | _not forced_ | `static/source_logos/coupangplay.svg` |
| `disney_plus` | `#01147C` | `rgba(255,255,255,0.18)` | `#FFFFFF` | `static/source_logos/disney_plus.svg` |
| `laftel` | `#816EEB` | `rgba(255,255,255,0.20)` | `#FFFFFF` | _kept as existing inline fallback_ |
| `watcha` | `#FF0558` | `rgba(255,255,255,0.18)` | `#FFFFFF` | _existing inline fallback_ |

## Notes

- Runtime logo loading is local-only (`/static/source_logos/*`).
- Missing assets fall back to inline SVG or text fallback by design.
- Legacy source IDs (for example `disney`) are normalized to `disney_plus` in UI state and filtering.
