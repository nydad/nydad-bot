# NYD. Daily Digest

매일 오전 7시 + 오후 12:10, AI가 자동으로 큐레이션하는 투자·코인·AI·KBO 다이제스트.

**Live**: https://nydad.github.io/nydad-bot/

## 구조

```
nydad-bot/
├── index.html                # 프론트엔드 (Paper Ledger 테마)
├── app.js                    # 프론트엔드 JS (5탭 렌더링, 실시간 인사이트)
├── scripts/
│   ├── collect_news.py       # 7AM 메인 파이프라인 (5탭 데이터 수집 + AI 에디토리얼)
│   ├── domestic_analysis.py  # 국내 투자 분석 (상관관계, 외국인 수급, AI 인사이트)
│   ├── midday_analysis.py    # 12:10PM 오후 시황 (11시 캔들, 장중 수급, 오전 뉴스)
│   ├── kbo_collect.py        # KBO 순위/경기/뉴스 스크래핑
│   ├── sync_market.py        # 실시간 시세 동기화
│   ├── backtest_*.py         # 백테스트 도구 (상관계수, 11시 캔들, 시그널 밸런스)
│   └── requirements.txt      # Python 의존성
├── workers/
│   └── insight-api/          # Cloudflare Worker (실시간 인사이트 API)
│       ├── src/index.js
│       └── wrangler.toml
├── data/
│   ├── index.json            # 날짜 인덱스
│   ├── YYYY-MM-DD.json       # 일별 다이제스트 데이터
│   └── sector_correlations.json  # 섹터별 상관계수 백테스트 결과
└── .github/workflows/
    ├── daily-digest.yml      # 7AM + 12:10PM KST 자동 실행
    ├── deploy-only.yml       # 프론트엔드만 빠른 배포
    └── sync-market.yml       # 수동 시세 동기화
```

## 5개 탭

| 탭 | 데이터 소스 | AI 생성 |
|----|------------|---------|
| **국내 투자** | yfinance (30+ 티커), KRX, RSS 15개 | 방향성 시그널 (가중치 기반), 섹터 상관관계, 외국인 수급, 오후 시황 |
| **코인 투자** | CoinGecko, Fear & Greed, RSS 9개 | 브리핑, 핵심 이벤트 |
| **AI 업계** | RSS 11개 (TechCrunch, Verge 등) | 브리핑, 주요 발언 |
| **AI 코딩** | RSS 10개 (GitHub, arXiv 등) | 브리핑, 하이라이트 |
| **KBO** | KBO 공식 사이트, 스포츠 RSS 3개 | 브리핑, 순위표, 경기 스코어 |

## 핵심 기능

### 장전 시황 (7AM KST)
- **가중치 기반 방향성 시그널** — 야간선물 2.0x, VIX/SOX/환율 1.5x, 유가/금 0.8x, 뉴스 0.5x
- **시가 예상 표현** — "강한 상승 출발", "하락 출발 예상" 등 야간선물 기반
- **섹터 상관관계** — 백테스트 검증된 US→KR 1일 시차 상관계수 기반
- **외국인 수급** — KRX API 금액(억원) 기반 (주수가 아닌 실제 거래대금)
- **오답노트** — 시초가 대비 종가 기준 판정 (트레이딩 관점)

### 오후 시황 (12:10PM KST)
- **11시 60분봉 분석** — 양봉 → 오후 상승 71% (백테스트 검증)
- **장중 외국인 수급** — KRX 실시간 매매 동향
- **오전 뉴스 정리** — 국내 RSS 4개 (한경, 매경, 이데일리, 연합인포맥스)
- **7시 예측 검증** — 오전 예측 vs 실제 비교 + 오후 전망 업데이트

### 섹터 상관관계 (백테스트 검증, 1일 시차)

| 섹터 | US 리더 | KR 연동 | 시차 상관계수 |
|------|---------|---------|:---:|
| 메모리/반도체 | WDC, MU, LRCX, AMAT, SOX | 삼성전자, SK하이닉스 | 0.72~0.80 |
| 2차전지/EV | TSLA, SQM, ALB | LG에너지, 삼성SDI | 0.69 |
| 전력망 | NRG, VST | HD현대일렉 | 0.70~0.72 |
| 방산/우주 | LMT, RTX, RKLB | 한화에어로 | 0.35~0.50 |
| 로봇 | ISRG, ROK | KOSPI 연동 | 0.47 |

### 실시간 인사이트 (Cloudflare Worker)
국내 투자 탭에서 질문을 입력하면 실시간 분석:
- Yahoo Finance v8 API로 30+ 티커 실시간 조회 (메모리, 2차전지, 방산, 로봇 포함)
- 섹터별 패턴 분석 — `sectorSignal()` 헬퍼로 통합 관리
- 중복 집계 방지 — 글로벌 리스크오프 시 개별 팩터 제거
- 하루 5회 글로벌 제한 (Durable Object 기반)

**패턴 분석기:**
1. 장중 모멘텀 (KOSPI ±0.5%)
2. 야간선물 (ES/NQ) — KOSPI 시가 상관계수 r=0.78
3. VIX 레짐 (bullish < 20, bearish > 25/30)
4. 환율 (USD/KRW ±0.3%)
5. SOX 반도체 — KOSPI 시가 상관계수 r=0.85 (최강 선행지표)
6. 메모리 섹터 (MU/WDC/AMAT/LRCX 평균)
7. 2차전지 (TSLA/ALB/ENPH 평균)
8. 방산/우주 (LMT/RTX/RKLB 평균)
9. 로봇 (ISRG/ROK 평균)
10. 유가 (급등 bearish, 급락 bullish — 한국 순수입국)
11. 금/채권/DXY
12. KOSPI vs KOSDAQ 괴리
13. EWY-KOSPI 괴리 (외국인 선행)
14. 글로벌 리스크 복합 시그널

## Short bias 수정 이력

기존 시스템은 20일 중 19일 SHORT을 추천 (95% short bias). 원인과 수정 사항:

| 원인 | 수정 |
|------|------|
| AI 실패 시 기본값 `short` | 패턴 기반 방향 판단으로 변경 |
| VIX bullish < 18 (비현실) | 20으로 상향 (대칭화) |
| 유가 급락 = bearish | bullish로 수정 (순수입국) |
| 지정학 8건=critical | regex 단어경계 + 가중치 방식으로 전환 |
| geo critical + bear≥3 → short 강제 | override 제거 |
| 뉴스-가격 이중 카운트 | 뉴스 가중치 50% 감산 |
| 전일 KOSPI/S&P 종가 = bearish 1표 | neutral 처리 (후행 지표) |
| EWY 프록시 기본 sell | ambiguous → unknown |
| Equal-vote (1:1) | 가중치 합산 (선물 2.0x ~ 뉴스 0.5x) |
| 오답노트 전일 종가 기준 | 시초가 대비 종가 기준 |

## 기술 스택

| 구성 | 기술 |
|------|------|
| 프론트엔드 | Vanilla JS, CSS (Paper Ledger 테마), 모바일 하단 독 |
| 백엔드 | Python 3.12 (yfinance, feedparser, trafilatura, BeautifulSoup) |
| AI 모델 | Gemini Flash (요약), Claude Sonnet (에디토리얼) via OpenRouter |
| AI 프롬프트 | 영어 (출력만 한국어) — AI 이해도 최적화 |
| 실시간 API | Cloudflare Workers + Durable Objects (5회/일 글로벌 제한) |
| 배포 | GitHub Pages + GitHub Actions |
| 데이터 | 정적 JSON (30일 롤링), 실시간은 Worker |

## 설정

### 1. GitHub Secrets
```
OPENROUTER_API_KEY=sk-or-...
COINGECKO_DEMO_API_KEY=cg-...
```

### 2. GitHub Variables (선택)
```
OPENROUTER_MODEL_FAST=google/gemini-3-flash-preview
OPENROUTER_MODEL_QUALITY=anthropic/claude-sonnet-4.6
```

### 3. Cloudflare Worker
```bash
cd workers/insight-api
npx wrangler login
npx wrangler secret put OPENROUTER_API_KEY
npx wrangler deploy
```

### 4. 로컬 실행
```bash
pip install -r scripts/requirements.txt
echo "OPENROUTER_API_KEY=sk-or-..." > .env
python scripts/collect_news.py          # 전체 다이제스트 (7AM)
python scripts/midday_analysis.py       # 오후 시황 (12:10PM)
python scripts/backtest_11am_candle.py  # 11시 캔들 백테스트
python scripts/backtest_sector_correlations.py  # 섹터 상관계수
```

## 배포

| 워크플로우 | 트리거 | 용도 |
|-----------|--------|------|
| `daily-digest.yml` | 22:00 UTC (7AM KST) | 장전 시황 + 전체 다이제스트 |
| `daily-digest.yml` | 03:10 UTC (12:10PM KST, 평일) | 오후 시황 (11시 캔들 마감 후) |
| `deploy-only.yml` | 수동 | 프론트엔드만 배포 |
| `sync-market.yml` | 수동 | 실시간 시세 업데이트 |

## 디자인

**Paper Ledger** — 에디토리얼 라이트 테마
- 배경: #F3EEE2 (따뜻한 종이색)
- 서체: IBM Plex Sans KR (UI) + Source Serif 4 (에디토리얼) + IBM Plex Mono (데이터)
- 강세: #0D8A63 (딥 그린) / 약세: #BA4A31 (테라코타 레드)
- 다크 모드: Copper Night (#15181E) — 우상단 토글

## 라이선스

MIT
