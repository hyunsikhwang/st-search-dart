# 📊 DART 재무정보 조회 (st-search-dart)

Open DART API를 활용하여 국내 상장 기업의 최근 4년(16분기) 재무 추이를 시각화해주는 Streamlit 웹 애플리케이션입니다.

## ✨ 주요 기능

- **기업 검색**: 기업명 입력 시 Open DART 고유번호를 자동으로 검색 및 매칭합니다.
- **포괄적인 데이터 수집**: 최근 4개년(최대 16분기)의 매출액, 영업이익 데이터를 한 번에 조회합니다.
- **데이터 정제 (Q4 조정)**: DART API에서 제공하는 4분기 누적 데이터를 분석하여 실제 4분기 단일 기간의 값을 자동으로 계산합니다.
- **고성능 캐싱 (DuckDB)**: 한 번 조회한 데이터는 로컬 DuckDB에 저장되어, 동일한 데이터 요청 시 API 호출 없이 즉시 표시됩니다.
- **인터랙티브 시각화**:
  - **Plotly Chart**: 매출액, 영업이익, 영업이익률 추이를 한눈에 볼 수 있는 복합 차트를 제공합니다.
  - **Great Tables**: 프리미엄 스타일이 적용된 데이터 테이블을 통해 수치 정보를 명확하게 제공합니다.
- **반응형 UI**: 데스크톱과 모바일 환경에 최적화된 레이아웃을 지원합니다.

## 🛠 기술 스택

- **Frontend**: [Streamlit](https://streamlit.io/)
- **Data Analysis**: [Pandas](https://pandas.pydata.org/)
- **Database**: [DuckDB](https://duckdb.org/)
- **Visualization**: [Plotly](https://plotly.com/python/), [Great Tables](https://posit-dev.github.io/great-tables/articles/intro.html)
- **API**: [Open DART API](https://opendart.fss.or.kr/)

## 🚀 시작하기

### 1. 필수 패키지 설치

```bash
pip install -r requirements.txt
```

### 2. API 키 설정

Open DART에서 발급받은 API 키가 필요합니다. 다음 중 하나의 방법으로 설정할 수 있습니다.

#### 방법 A: Streamlit Secrets (추천)
`.streamlit/secrets.toml` 파일을 생성하고 키를 입력합니다.
```toml
DART_API_KEY = "your_api_key_here"
```

#### 방법 B: 환경 변수
```bash
export DART_API_KEY="your_api_key_here"
```

### 3. 애플리케이션 실행

```bash
streamlit run app.py
```

## 📖 사용 방법

1. 앱 실행 후 **회사명**을 입력합니다 (예: 삼성전자).
2. **기준 연월**을 YYYYMM 형식으로 입력합니다 (예: 202412).
3. **조회하기** 버튼을 클릭하면 데이터 수집 및 분석이 시작됩니다.
4. 분석이 완료되면 좌측에는 상세 데이터 테이블이, 우측에는 추이 차트가 표시됩니다.

## ⚠️ 참고 사항

- 이 도구는 Open DART에서 제공하는 `전체 재무제표` API를 사용하므로, XBRL 제출 대상이 아닌 소규모 기업의 경우 데이터 조회가 제한될 수 있습니다.
- 조회된 재무 데이터는 실행 경로의 `financial_data.duckdb` 파일에 저장됩니다.

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.
lit cloud
