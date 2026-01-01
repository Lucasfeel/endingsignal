#!/usr/bin/env bash
set -euo pipefail

UA='Mozilla/5.0'
ORIGIN='https://webtoon.kakao.com'
BASE='https://gateway-kw.kakao.com/section/v1/pages'

echo "[1-A] Checking Set-Cookie from homepage"
curl -sS -D - -o /dev/null \
  -H "User-Agent: ${UA}" \
  -H "Referer: ${ORIGIN}/" \
  -H 'Accept-Language: ko-KR,ko;q=0.9' \
  "${ORIGIN}/" | grep -i '^set-cookie' || echo 'NO_SET_COOKIE'

echo "\n[1-B] Looking for cookie strings inside HTML"
curl -sS \
  -H "User-Agent: ${UA}" \
  -H "Referer: ${ORIGIN}/" \
  -H 'Accept-Language: ko-KR,ko;q=0.9' \
  "${ORIGIN}/" | grep -E 'webid|_T_ANO|document\.cookie' | head -n 50 || true

probe () {
  local path="$1"
  local cookie="$2"
  local code
  if [ -n "$cookie" ]; then
    code=$(curl -s -o /dev/null -w '%{http_code}' \
      -H "User-Agent: $UA" \
      -H "Referer: $ORIGIN/" \
      -H "Origin: $ORIGIN" \
      -H "Accept: application/json" \
      -H "Cookie: $cookie" \
      "$BASE/$path" )
  else
    code=$(curl -s -o /dev/null -w '%{http_code}' \
      -H "User-Agent: $UA" \
      -H "Referer: $ORIGIN/" \
      -H "Origin: $ORIGIN" \
      -H "Accept: application/json" \
      "$BASE/$path" )
  fi
  echo "$path -> $code"
}

echo "\n[1-C] Probing API endpoints with/without cookies"
probe 'general-weekdays' ''
probe 'completed?offset=0&limit=1' ''
probe 'completed?offset=0&limit=1' 'webid=deadbeef; _T_ANO=deadbeef'

echo "\n[1-D] aiohttp cookie jar check"
python - <<'PY'
import asyncio
import aiohttp

URL = 'https://webtoon.kakao.com/'

async def main():
    jar = aiohttp.CookieJar(unsafe=True)
    async with aiohttp.ClientSession(cookie_jar=jar) as s:
        async with s.get(URL, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://webtoon.kakao.com/',
            'Accept-Language': 'ko-KR,ko;q=0.9',
        }, allow_redirects=True) as r:
            await r.text()
            print('status:', r.status)
            print('final_url:', str(r.url))
            sc = r.headers.getall('Set-Cookie', [])
            print('set_cookie_count:', len(sc))
            print('set_cookie_names:', sorted({c.split('=',1)[0].strip() for c in sc if '=' in c})[:50])

        ck = s.cookie_jar.filter_cookies(URL)
        keys = sorted(list(ck.keys()))
        print('cookiejar_keys:', keys)
        print('has_webid:', 'webid' in ck)
        print('has__T_ANO:', '_T_ANO' in ck)

asyncio.run(main())
PY
