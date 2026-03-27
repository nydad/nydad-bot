# nydad. Daily Digest

매일 오전 7시, AI가 자동으로 큐레이션하는 투자·코인·AI·KBO 다이제스트.

**Live**: https://nydad.github.io/nydad-bot/

## 구조

```
nydad-bot/
├── index.html             # 프론트엔드 (Paper Ledger 테마, 인라인 CSS)
├── app.js                 # 프론트엔드 JS (5탭 렌더링, 인사이트 버튼)
├── scripts/
│   ├── collect_news.py    # 메인 파이프라인 (5탭 데이터 수집 + AI 생성)
│   ├── domestic_analysis.py  # 국내 투자 분석 (상관관계, 외국인 수급, AI 인사이트)
│   ├── kbo_collect.py     # KBO 순위/경기/뉴스 스크래핑
│   ├── sync_market.py     # 실시간 시세 동기화
│   └── requirements.txt   # Python 의존성
├── workers/
│   └── insight-api/       # Cloudflare Worker (실시간 인사이트 API)
│       ├── src/index.js
│       └── wrangler.toml
├── data/
│   ├── index.json         # 날짜 인덱스
│   └── YYYY-MM-DD.json    # 일별 다이제스트 데이터
└── .github/workflows/
    ├── daily-digest.yml   # 매일 7AM KST 자동 실행
    ├── deploy-only.yml    # 프론트엔드만 빠른 배포 (2분)
    └── sync-market.yml    # 수동 시세 동기화
```

## 5개 탭

| 탭 | 데이터 소스 | AI 생성 |
|----|------------|---------|
| **국내 투자** | yfinance (20+ 티커), KRX, RSS 15개 | 방향성 시그널 (LONG/SHORT %), 상관관계, 외국인 수급, 인사이트 |
| **코인 투자** | CoinGecko, Fear & Greed, RSS 9개 | 브리핑, 핵심 이벤트 |
| **AI 업계** | RSS 11개 (TechCrunch, Verge 등) | 브리핑, 주요 발언 |
| **AI 코딩** | RSS 10개 (GitHub, arXiv 등) | 브리핑, 하이라이트 |
| **KBO** | KBO 공식 사이트, 스포츠 RSS 3개 | 브리핑, 순위표, 경기 스코어 |

## 핵심 기능

### 투자 시그널
- **LONG/SHORT 퍼센티지** — 중립 없음, 반드시 방향 제시
- **상관관계 분석** — NVDA↔삼성전자, MU↔SK하이닉스 등 20일 롤링 상관계수
- **외국인 수급** — KRX/Naver Finance 기반 순매수/순매도 추적
- **오답노트** — 전일 시그널 정확도 검증 + 원인 분석

### 실시간 인사이트 (Cloudflare Worker)
국내 투자 탭에서 질문을 입력하면 실시간 분석:
- Yahoo Finance v8 API로 20개 티커 실시간 조회
- 10개 패턴 자동 분석 (야간선물, VIX, 환율, SOX, NVDA/MU 등)
- Claude Sonnet AI가 종합 판단 → LONG/SHORT % + 근거

**패턴 분석기 목록:**
1. 장중 모멘텀 (적중률 58%, Lai et al. 2022)
2. 야간선물 방향성 (ES/NQ/YM)
3. VIX 레짐 시그널
4. 환율 압력 (USD/KRW)
5. SOX → 한국 반도체 스필오버
6. NVDA/MU → 삼성/하이닉스 선행
7. 유가 충격 분류
8. 금 리스크 시그널
9. 미국 10년물 금리 방향
10. 갭 반전 패턴 (장중 한정)
11. 미국 장 마감 → 내일 전망 (장 마감 후)

시간대 인식: 정규장(09:00-15:30), NXT장(08:00-20:00), 주말/휴장 자동 감지.

## 기술 스택

| 구성 | 기술 |
|------|------|
| 프론트엔드 | Vanilla JS, CSS (Paper Ledger 테마), 모바일 하단 독 |
| 백엔드 | Python 3.12 (yfinance, feedparser, trafilatura, BeautifulSoup) |
| AI 모델 | Gemini Flash (요약), Claude Sonnet (에디토리얼) via OpenRouter |
| 실시간 API | Cloudflare Workers (Yahoo Finance + OpenRouter 프록시) |
| 배포 | GitHub Pages + GitHub Actions |
| 데이터 | 정적 JSON (30일 롤링), 실시간은 Worker에서 처리 |

## 설정 방법

### 1. GitHub Secrets
```
OPENROUTER_API_KEY=sk-or-...
```

### 2. GitHub Variables (선택)
```
OPENROUTER_MODEL_FAST=google/gemini-3-flash-preview
OPENROUTER_MODEL_QUALITY=anthropic/claude-sonnet-4.6
```

### 3. Cloudflare Worker (실시간 인사이트)
```bash
cd workers/insight-api
npx wrangler login
npx wrangler secret put OPENROUTER_API_KEY  # OpenRouter 키 입력
npx wrangler deploy
```

배포 후 `app.js`의 `INSIGHT_API` 변수에 Worker URL 설정:
```js
var INSIGHT_API = "https://nydad-insight-api.nydad.workers.dev";
```

### 4. 로컬 실행
```bash
pip install -r scripts/requirements.txt
echo "OPENROUTER_API_KEY=sk-or-..." > .env
python scripts/collect_news.py  # 전체 다이제스트 생성
python scripts/kbo_collect.py --json  # KBO만 테스트
```

## 배포

| 워크플로우 | 트리거 | 소요 시간 | 용도 |
|-----------|--------|----------|------|
| `daily-digest.yml` | 매일 22:00 UTC (= 07:00 KST) | 15-20분 | 전체 다이제스트 생성 + 배포 |
| `deploy-only.yml` | 수동 | 2분 | 프론트엔드만 배포 (데이터 수집 없음) |
| `sync-market.yml` | 수동 | 1분 | 실시간 시세만 업데이트 |

수동 배포: GitHub Actions → "Deploy Only" → "Run workflow"

## 디자인

**Paper Ledger** — 에디토리얼 라이트 테마
- 배경: #F3EEE2 (따뜻한 종이색)
- 서체: IBM Plex Sans KR (UI) + Source Serif 4 (에디토리얼) + IBM Plex Mono (데이터)
- 강세: #0D8A63 (딥 그린) / 약세: #BA4A31 (테라코타 레드)
- 다크 모드: Copper Night (#15181E) — 우상단 토글

## Cloudflare Worker 상세

**목적**: GitHub Pages는 정적 사이트라 서버사이드 로직이 불가능. Cloudflare Worker가 실시간 API 역할:
1. 브라우저에서 POST 요청 (사용자 질문 포함)
2. Worker가 Yahoo Finance v8 API로 20개 티커 실시간 조회
3. 10개 패턴 분석기 실행
4. OpenRouter API로 Claude Sonnet에 컨텍스트 전달
5. AI 분석 결과를 JSON으로 반환

**비용**: Cloudflare Workers 무료 티어 (일 100,000 요청). OpenRouter API 비용만 발생.

**보안**: OpenRouter API 키는 Worker의 secret에 저장 (브라우저 노출 없음). CORS는 `nydad.github.io`만 허용.

## 라이선스

MIT
