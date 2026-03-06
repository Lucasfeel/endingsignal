FROM node:24-alpine AS frontend-build

WORKDIR /app

COPY package.json tsconfig.json tailwind.config.cjs postcss.config.cjs vite.config.ts ./
COPY frontend ./frontend
COPY templates/index.html ./templates/index.html
COPY static/app.js ./static/app.js
COPY static/es_theme_toss.css ./static/es_theme_toss.css

RUN npm install \
    && npm run build

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir playwright \
    && python -m playwright install --with-deps chromium

COPY . .
COPY --from=frontend-build /app/static/build ./static/build

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import os, sys, urllib.request; port = os.getenv('PORT', '5000'); url = f'http://127.0.0.1:{port}/healthz'; sys.exit(0) if urllib.request.urlopen(url, timeout=3).status == 200 else sys.exit(1)"

CMD ["python", "scripts/start_web.py"]
