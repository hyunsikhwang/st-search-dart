import streamlit as st
import pandas as pd
import duckdb
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import os
import concurrent.futures
import time
import plotly.graph_objects as go
import great_tables as gt
from typing import Optional, Dict, List

# ==========================================
# 0. Streamlit 설정 및 상수
# ==========================================
st.set_page_config(
    page_title="DART 재무정보 검색 | Value Horizon",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# API 키 가져오기 (Streamlit Secrets 우선, 없으면 환경변수)
try:
    API_KEY = st.secrets["DART_API_KEY"]
except (FileNotFoundError, KeyError):
    API_KEY = os.getenv("DART_API_KEY")

if API_KEY:
    API_KEY = API_KEY.strip()

# MotherDuck 설정
try:
    MD_TOKEN = st.secrets["MOTHERDUCK_TOKEN"]
except (FileNotFoundError, KeyError):
    MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

if MD_TOKEN:
    MD_TOKEN = MD_TOKEN.strip()
    # MotherDuck 연결 (토큰이 있으면 md: prefix 사용)
    # [수정] 특정 DB를 지정해서 연결하면 해당 DB가 없을 때 오류가 발생함.
    # md: 만 지정하여 연결 후 init_db에서 DB를 생성하거나 선택하도록 함.
    DB_PATH = "md:"
else:
    # 로컬 DuckDB 연결
    DB_PATH = "financial_data.duckdb"

# ==========================================
# 1. Database 초기화
# ==========================================
def init_db():
    try:
        if MD_TOKEN:
            # MotherDuck 연결 시 토큰을 config로 전달
            conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
            # MotherDuck에서는 DB가 없을 수 있으므로 생성 및 사용 설정
            conn.execute("CREATE DATABASE IF NOT EXISTS dart_financials")
            conn.execute("USE dart_financials")
        else:
            conn = duckdb.connect(DB_PATH)
            
        # 재무정보 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cached_financials (
                corp_code VARCHAR,
                year INTEGER,
                quarter INTEGER,
                report_code VARCHAR,
                fs_div VARCHAR,
                account_id VARCHAR,
                account_nm VARCHAR,
                thstrm_amount BIGINT,
                PRIMARY KEY (corp_code, year, report_code, fs_div, account_id)
            )
        """)

        # 회사 고유번호 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS corp_codes (
                corp_code VARCHAR PRIMARY KEY,
                corp_name VARCHAR,
                stock_code VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 처리 상태 테이블
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_status (
                corp_code VARCHAR,
                corp_name VARCHAR,
                last_base_period VARCHAR,
                status VARCHAR DEFAULT 'SUCCESS',
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (corp_code)
            )
        """)
        conn.close()
    except Exception as e:
        st.error(f"데이터베이스 초기화 중 오류가 발생했습니다: {e}")

# 앱 실행 시 DB 초기화
init_db()

# ==========================================
# 2. DART 고유번호(Corp Code) 관리 (Cached)
# ==========================================

def sync_corp_codes_from_api(api_key: str):
    """Open DART에서 고유번호를 다운로드하여 DB에 저장합니다."""
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {'crtfc_key': api_key}

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_filename = zip_file.namelist()[0]
                with zip_file.open(xml_filename) as f:
                    tree = ET.parse(f)
                    root = tree.getroot()

                    data_list = []
                    for corp in root.findall('.//list'):
                        code = corp.findtext('corp_code', '').strip()
                        name = corp.findtext('corp_name', '').strip()
                        stock = corp.findtext('stock_code', '').strip()
                        # 주식 코드가 있는(상장사) 경우에만 추가
                        if code and name and stock:
                            data_list.append((code, name, stock))

            if data_list:
                # 리스트를 DataFrame으로 변환 (성공적인 벌크 삽입을 위해)
                df = pd.DataFrame(data_list, columns=['corp_code', 'corp_name', 'stock_code'])
                
                if MD_TOKEN:
                    conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
                    conn.execute("USE dart_financials")
                else:
                    conn = duckdb.connect(DB_PATH)
                
                # [수정] 스키마 변경 시 컬럼 추가를 위해 처리
                try:
                    conn.execute("ALTER TABLE corp_codes ADD COLUMN IF NOT EXISTS stock_code VARCHAR")
                except:
                    pass

                # DuckDB의 강력한 기능을 활용해 DataFrame을 직접 테이블에 삽입 (매우 빠름)
                conn.execute("INSERT OR REPLACE INTO corp_codes (corp_code, corp_name, stock_code) SELECT corp_code, corp_name, stock_code FROM df")
                conn.close()
                return True
        return False
    except Exception as e:
        st.error(f"고유번호 동기화 실패: {e}")
        return False

@st.cache_data(ttl=3600*24)  # 24시간 캐시
def get_company_codes(api_key: str) -> Optional[Dict[str, str]]:
    """DB에서 고유번호를 읽어옵니다. DB에 없으면 API를 호출합니다."""
    try:
        if MD_TOKEN:
            conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
            conn.execute("USE dart_financials")
        else:
            conn = duckdb.connect(DB_PATH)
        
        df = conn.execute("SELECT corp_name, corp_code FROM corp_codes").df()
        conn.close()

        if df.empty:
            # DB가 비어있으면 API 호출 시도
            if sync_corp_codes_from_api(api_key):
                if MD_TOKEN:
                    conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
                    conn.execute("USE dart_financials")
                else:
                    conn = duckdb.connect(DB_PATH)
                df = conn.execute("SELECT corp_name, corp_code FROM corp_codes").df()
                conn.close()
        
        if not df.empty:
            return df.set_index('corp_name')['corp_code'].to_dict()
        return None
    except Exception as e:
        st.error(f"고유번호 로드 실패: {e}")
        return None

def search_company_code(api_key: str, company_name: str) -> Optional[str]:
    """회사명으로 고유번호를 검색합니다."""
    codes = get_company_codes(api_key)
    if not codes:
        return None

    # 1. 정확 일치
    if company_name in codes:
        return str(codes[company_name]).zfill(8)

    # 2. 부분 일치 검색
    candidates = [name for name in codes.keys() if company_name in name]
    if len(candidates) == 1:
        return str(codes[candidates[0]]).zfill(8)
    elif len(candidates) > 1:
        st.warning(f"검색 결과가 너무 많습니다. 더 정확한 이름을 입력해주세요. (후보: {', '.join(candidates[:5])}...)")
        return None
    else:
        return None

# ==========================================
# 3. 재무제표 데이터 수집 및 DB 관리
# ==========================================

def get_financial_data(api_key: str, corp_code: str, year: int, report_type: str, fs_div: str, session: requests.Session = None) -> Optional[pd.DataFrame]:
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        'crtfc_key': api_key,
        'corp_code': str(corp_code).zfill(8),
        'bsns_year': str(year),
        'reprt_code': report_type,
        'fs_div': fs_div
    }
    
    try:
        if session:
            res = session.get(url, params=params, timeout=10)
        else:
            res = requests.get(url, params=params, timeout=10)
        data = res.json()
        
        if data['status'] == '000' and data.get('list'):
            df = pd.DataFrame(data['list'])
            numeric_cols = ['thstrm_amount', 'frmtrm_amount', 'bfefrmtrm_amount']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
            return df
        return None
    except Exception:
        return None

def get_financial_data_from_db(corp_code: str, year: int, report_code: str, fs_div: str) -> Optional[pd.DataFrame]:
    try:
        if MD_TOKEN:
            conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
            conn.execute("USE dart_financials")
        else:
            conn = duckdb.connect(DB_PATH)
            
        query = """
            SELECT account_id, account_nm, thstrm_amount 
            FROM cached_financials 
            WHERE corp_code = ? AND year = ? AND report_code = ? AND fs_div = ?
        """
        df = conn.execute(query, [str(corp_code), int(year), str(report_code), str(fs_div)]).df()
        conn.close()
        return df if not df.empty else None
    except Exception:
        return None

def save_financial_data_to_db(df: pd.DataFrame, corp_code: str, year: int, quarter: int, report_code: str, fs_div: str):
    if df is None or df.empty:
        return

    try:
        if MD_TOKEN:
            conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
            conn.execute("USE dart_financials")
        else:
            conn = duckdb.connect(DB_PATH)
            
        key_items = ['ifrs-full_Revenue', 'dart_OperatingIncomeLoss']
        target_df = df[df['account_id'].isin(key_items)].copy()
        
        if target_df.empty:
            conn.close()
            return
            
        data_to_insert = []
        for _, row in target_df.iterrows():
            data_to_insert.append((
                str(corp_code),
                int(year),
                int(quarter),
                str(report_code),
                str(fs_div),
                row['account_id'],
                row['account_nm'],
                int(row['thstrm_amount']) if pd.notna(row['thstrm_amount']) else 0
            ))
            
        conn.executemany("""
            INSERT OR REPLACE INTO cached_financials 
            (corp_code, year, quarter, report_code, fs_div, account_id, account_nm, thstrm_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, data_to_insert)
        conn.close()
    except Exception as e:
        st.error(f"데이터베이스 저장 중 오류가 발생했습니다: {e}")

def get_quarter_info(year_month: int) -> tuple:
    year = year_month // 100
    month = year_month % 100
    if month <= 3: return 1, year, 3
    elif month <= 6: return 2, year, 6
    elif month <= 9: return 3, year, 9
    else: return 4, year, 12

def adjust_q4_values(df: pd.DataFrame) -> pd.DataFrame:
    """4분기 누적값을 실제 4분기 값으로 조정"""
    if df.empty or '분기' not in df.columns:
        return df

    q4_data = df[df['분기'] == 4].copy()
    if q4_data.empty:
        return df

    for year in q4_data['년도'].unique():
        q1_q3_data = df[(df['년도'] == year) & df['분기'].isin([1, 2, 3])]
        if q1_q3_data.empty:
            continue

        q1_q2_q3_sum = {}
        for item in q1_q3_data['항목'].unique():
            for fs_div in q1_q3_data['구분'].unique():
                item_sum = q1_q3_data[(q1_q3_data['항목'] == item) & (q1_q3_data['구분'] == fs_div)]['thstrm_amount'].sum()
                q1_q2_q3_sum[(year, item, fs_div)] = item_sum

        year_q4_data = df[(df['년도'] == year) & (df['분기'] == 4)]
        for idx, row in year_q4_data.iterrows():
            item = row['항목']
            fs_div = row['구분']
            if (year, item, fs_div) in q1_q2_q3_sum:
                df.at[idx, 'thstrm_amount'] = row['thstrm_amount'] - q1_q2_q3_sum[(year, item, fs_div)]

    return df

# ==========================================
# 4. Core Logic (Streamlit Status 연동)
# ==========================================

def collect_financials(api_key: str, corp_code: str, year_month: int) -> pd.DataFrame:
    corp_code = str(corp_code).zfill(8)
    report_types = [('사업보고서', '11011'), ('1분기보고서', '11013'), ('반기보고서', '11012'), ('3분기보고서', '11014')]
    fs_divs = [('연결', 'CFS'), ('별도', 'OFS')]
    
    quarter, quarter_end_year, quarter_end_month = get_quarter_info(year_month)
    start_year = quarter_end_year - 4
    
    # 수집할 분기 목록 생성
    quarters_to_collect = []
    curr_y, curr_q = start_year, 1
    end_y, end_q = quarter_end_year, quarter
    if quarter_end_month == 12: end_q = 4

    while True:
        quarters_to_collect.append((curr_y, curr_q))
        if curr_y == end_y and curr_q == end_q: break
        curr_q += 1
        if curr_q > 4:
            curr_q = 1
            curr_y += 1

    all_data = []
    missing_tasks = []
    determined_fs_divs = fs_divs 

    # Status 컨테이너
    status_text = st.empty()
    
    with requests.Session() as session:
        # 1. DB 조회
        for t_year, t_quarter in quarters_to_collect:
            if t_quarter == 1: r_code, r_name = '11013', '1분기보고서'
            elif t_quarter == 2: r_code, r_name = '11012', '반기보고서'
            elif t_quarter == 3: r_code, r_name = '11014', '3분기보고서'
            else: r_code, r_name = '11011', '사업보고서'

            found_in_db = False
            for fs_name, fs_code in determined_fs_divs:
                db_df = get_financial_data_from_db(corp_code, t_year, r_code, fs_code)
                if db_df is not None:
                    db_df['보고서명'] = r_name
                    db_df['구분'] = fs_name
                    db_df['년도'] = t_year
                    db_df['분기'] = t_quarter
                    all_data.append(db_df)
                    found_in_db = True
                    if fs_code == 'CFS' and len(determined_fs_divs) > 1:
                        determined_fs_divs = [('연결', 'CFS')]
                    break
            
            if not found_in_db:
                missing_tasks.append((t_year, t_quarter, r_code, r_name))

        # 2. API Probing & Fetching
        if missing_tasks:
            status_text.text(f"API 데이터 수집 중... ({len(missing_tasks)}건)")
            
            # Probing (연결/별도 확정)
            if len(determined_fs_divs) > 1:
                sorted_missing = sorted(missing_tasks, key=lambda x: (x[0], x[1]), reverse=True)
                for t_year, t_quarter, t_report_code, _ in sorted_missing:
                    cfs_df = get_financial_data(api_key, corp_code, t_year, t_report_code, 'CFS', session)
                    if cfs_df is not None:
                        determined_fs_divs = [('연결', 'CFS')]
                        save_financial_data_to_db(cfs_df, corp_code, t_year, t_quarter, t_report_code, 'CFS')
                        break
                    ofs_df = get_financial_data(api_key, corp_code, t_year, t_report_code, 'OFS', session)
                    if ofs_df is not None:
                        determined_fs_divs = [('별도', 'OFS')]
                        save_financial_data_to_db(ofs_df, corp_code, t_year, t_quarter, t_report_code, 'OFS')
                        break

            # 병렬 호출 준비
            api_tasks = []
            for t_year, t_quarter, t_report_code, t_report_name in missing_tasks:
                # Probing 후 DB 다시 확인
                found_after_probing = False
                for fs_name, fs_code in determined_fs_divs:
                    db_df_check = get_financial_data_from_db(corp_code, t_year, t_report_code, fs_code)
                    if db_df_check is not None:
                        db_df_check['보고서명'] = t_report_name
                        db_df_check['구분'] = fs_name
                        db_df_check['년도'] = t_year
                        db_df_check['분기'] = t_quarter
                        all_data.append(db_df_check)
                        found_after_probing = True
                        break
                
                if found_after_probing: continue

                for fs_name, fs_code in determined_fs_divs:
                    api_tasks.append({
                        'year': t_year, 'quarter': t_quarter, 'report_code': t_report_code,
                        'report_name': t_report_name, 'fs_code': fs_code, 'fs_name': fs_name
                    })

            # 병렬 실행
            if api_tasks:
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_task = {
                        executor.submit(get_financial_data, api_key, corp_code, t['year'], t['report_code'], t['fs_code'], session): t 
                        for t in api_tasks
                    }
                    for future in concurrent.futures.as_completed(future_to_task):
                        task = future_to_task[future]
                        try:
                            df = future.result()
                            if df is not None:
                                save_financial_data_to_db(df, corp_code, task['year'], task['quarter'], task['report_code'], task['fs_code'])
                                df['보고서명'] = task['report_name']
                                df['구분'] = task['fs_name']
                                df['년도'] = task['year']
                                df['분기'] = task['quarter']
                                all_data.append(df)
                        except Exception:
                            pass

    status_text.empty() # 상태 메시지 지우기

    if not all_data:
        return pd.DataFrame()

    # 데이터 정리
    combined = pd.concat(all_data, ignore_index=True)
    filtered = combined[['보고서명', '구분', 'account_id', 'account_nm', 'thstrm_amount', '년도', '분기']].copy()
    
    item_map = {'ifrs-full_Revenue': '매출액', 'dart_OperatingIncomeLoss': '영업이익'}
    filtered = filtered[filtered['account_id'].isin(item_map.keys())]
    filtered['항목'] = filtered['account_id'].map(item_map)

    # Q4 조정
    result_df = adjust_q4_values(filtered)
    
    # 처리 상태 업데이트 (데이터가 있든 없든 시도 기록)
    try:
        if MD_TOKEN:
            conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
            conn.execute("USE dart_financials")
        else:
            conn = duckdb.connect(DB_PATH)
        
        # [수정] 스키마 변경 시 컬럼 추가
        try:
            conn.execute("ALTER TABLE processing_status ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'SUCCESS'")
        except:
            pass

        # 회사명 가져오기
        codes_dict = get_company_codes(api_key)
        company_name_found = "알수없음"
        if codes_dict:
            for name, code in codes_dict.items():
                if code == corp_code:
                    company_name_found = name
                    break

        # 데이터 존재 여부에 따른 상태 설정
        current_status = 'SUCCESS' if not result_df.empty else 'NOT_FOUND'

        conn.execute("""
            INSERT OR REPLACE INTO processing_status (corp_code, corp_name, last_base_period, status, processed_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [corp_code, company_name_found, str(year_month), current_status])
        conn.close()
    except Exception:
        pass

    return result_df

def process_dataframe_for_view(df: pd.DataFrame) -> pd.DataFrame:
    """Streamlit 표시용 데이터프레임으로 변환"""
    if df.empty:
        return pd.DataFrame()

    pivot_df = df.pivot_table(
        index=['년도', '분기'],
        columns='항목',
        values='thstrm_amount',
        aggfunc='first'
    ).reset_index()

    # 정렬
    pivot_df = pivot_df.sort_values(by=['년도', '분기'], ascending=[True, True])

    # 기간 컬럼 생성
    pivot_df['기간'] = pivot_df.apply(lambda x: f"{int(x['년도'])}년 {int(x['분기'])}분기", axis=1)

    # 영업이익률 계산
    pivot_df['매출액'] = pivot_df['매출액'].fillna(0)
    pivot_df['영업이익'] = pivot_df['영업이익'].fillna(0)
    
    pivot_df['영업이익률'] = pivot_df.apply(
        lambda row: (row['영업이익'] / row['매출액'] * 100) if row['매출액'] != 0 else 0, axis=1
    )

    # 컬럼 순서 정리 및 단위 변환 (백만원)
    result_df = pivot_df[['기간', '매출액', '영업이익', '영업이익률']].copy()
    result_df['매출액'] = result_df['매출액'] / 1000000
    result_df['영업이익'] = result_df['영업이익'] / 1000000
    
    return result_df

# ==========================================
# 5. UI Layout - Value Horizon Design System
# ==========================================

# Custom CSS for Value Horizon Look & Feel
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

    /* Minimize Streamlit Padding and Margins */
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 3rem !important;
        max-width: 1000px !important;
    }
    
    [data-testid="stHeader"] {
        display: none;
    }

    /* Global Styles */
    .stApp {
        background-color: #ffffff;
        color: #1a1a1a;
        font-family: 'Inter', sans-serif;
    }

    /* Hero Section */
    .hero-container {
        padding: 2.5rem 0;
        text-align: center;
        border-bottom: 1px solid #f0f0f0;
        margin-bottom: 3rem;
    }

    .hero-title {
        font-size: 2.6rem;
        font-weight: 700;
        color: #111111;
        margin-bottom: 0.5rem;
        letter-spacing: -1px;
    }

    .hero-subtitle {
        font-size: 1.05rem;
        font-weight: 400;
        color: #888888;
        max-width: 600px;
        margin: 0 auto;
        line-height: 1.5;
    }

    /* Search Section Styling */
    .search-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #111111;
        margin-bottom: 1.25rem;
        display: flex;
        align-items: center;
        gap: 0.6rem;
    }

    /* Input and Button Refinement */
    div[data-testid="stForm"] {
        border: 1px solid #eaeaea !important;
        border-radius: 20px !important;
        padding: 2rem !important;
        background-color: #ffffff;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
    }

    .stTextInput input {
        border-radius: 10px !important;
        border: 1px solid #e0e0e0 !important;
        padding: 0.6rem 1rem !important;
        font-size: 1rem !important;
        background-color: #f9f9f9 !important;
        transition: all 0.2s ease;
    }

    .stTextInput input:focus {
        border-color: #007aff !important;
        background-color: #ffffff !important;
        box-shadow: 0 0 0 3px rgba(0,122,255,0.1) !important;
    }

    .stButton button {
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        background-color: #007aff !important;
        color: white !important;
        border: none !important;
        padding: 0.6rem 1.5rem !important;
        transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
        height: 100% !important;
    }

    .stButton button:hover {
        background-color: #0063cc !important;
        transform: translateY(-1px);
        box-shadow: 0 6px 15px rgba(0,122,255,0.25) !important;
    }

    /* Hide Streamlit components */
    #MainMenu, footer, header, .stDeployButton {
        display: none !important;
    }

    /* Expander Styling */
    .stExpander {
        border: 1px solid #f0f0f0 !important;
        border-radius: 12px !important;
        background-color: #fafafa !important;
        margin-top: 1rem;
    }
    
    .stExpander summary {
        font-weight: 600 !important;
        color: #666666 !important;
    }
</style>
""", unsafe_allow_html=True)

# Hero Section
st.markdown("""
<div class="hero-container">
    <div class="hero-title">📈 Search DART</div>
    <div class="hero-subtitle">Value Horizon의 스마트한 기업 분석 엔진.<br>실시간 공시 데이터를 바탕으로 최근 4개년의 핵심 재무 추이를 시각화합니다.</div>
</div>
""", unsafe_allow_html=True)

if not API_KEY:
    st.error("🚨 DART API Key가 설정되지 않았습니다. Streamlit Secrets에 `DART_API_KEY`를 설정해주세요.")
    st.stop()

# 모바일 여부 감지 함수
def is_mobile():
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        ctx = get_script_run_ctx()
        if ctx is None:
            return False
        session_info = ctx.session_info
        if session_info is None:
            return False
        user_agent = session_info.request.headers.get('User-Agent', '')
        return 'Mobile' in user_agent or 'Android' in user_agent or 'iPhone' in user_agent
    except:
        return False

# 검색 폼 (사이드바 대신 메인 영역에 배치)
st.markdown('<div class="search-header">🔍 검색 설정</div>', unsafe_allow_html=True)

with st.form(key="search_form"):
    # [수정] vertical_alignment="bottom" 적용
    # 텍스트 인풋(라벨 있음)과 버튼(라벨 없음)의 밑선을 맞춤
    col1, col2, col3 = st.columns([3, 2, 1], vertical_alignment="bottom")
    
    with col1:
        company_name = st.text_input("회사명", placeholder="예: 삼성전자", key="company_input")
    with col2:
        year_month = st.text_input("기준 연월 (YYYYMM)", value="202509", placeholder="202509", key="year_month_input")
    with col3:
        search_btn = st.form_submit_button("조회하기", type="primary", use_container_width=True, key="search_button")

st.markdown("---")
st.caption("Data source: Open DART API")

with st.expander("⚙️ 설정"):
    if st.button("🔄 회사 고유번호(corpCode.xml) 강제 갱신"):
        with st.spinner("Open DART에서 데이터를 가져오고 있습니다..."):
            if sync_corp_codes_from_api(API_KEY):
                st.success("회사 고유번호가 성공적으로 업데이트되었습니다.")
                st.cache_data.clear()
            else:
                st.error("회사 고유번호 업데이트에 실패했습니다.")

if search_btn and company_name and year_month:
    if not year_month.isdigit() or len(year_month) != 6:
        st.error("기준 연월은 YYYYMM 형식의 6자리 숫자여야 합니다.")
    else:
        with st.status("데이터를 조회하고 있습니다...", expanded=True) as status:
            company_search_status = st.empty()
            company_search_status.write("🏢 기업 고유번호 검색 중...")
            if API_KEY is None:
                status.update(label="❌ API 키 오류", state="error")
                st.error("DART API 키가 설정되지 않았습니다.")
            else:
                corp_code = search_company_code(API_KEY, company_name)

            if not corp_code:
                status.update(label="❌ 회사를 찾을 수 없습니다.", state="error")
                st.error(f"'{company_name}' 회사를 찾을 수 없습니다.")
            else:
                company_search_status.empty()
                company_search_status.write(f"✅ 고유번호 확인: {corp_code}")
                financial_status = st.empty()
                financial_status.write("📥 재무 데이터 수집 및 분석 중...")

                start_time = time.time()
                try:
                    raw_df = collect_financials(API_KEY, corp_code, int(year_month))
                    financial_status.empty()

                    if raw_df.empty:
                        status.update(label="❌ 데이터 없음", state="error")
                        st.warning("해당 기간의 재무 데이터를 찾을 수 없습니다.")
                    else:
                        view_df = process_dataframe_for_view(raw_df)
                        elapsed = time.time() - start_time

                        status.update(label=f"✅ 조회 완료! ({elapsed:.2f}초)", state="complete")

                        st.markdown(f"### 🏢 {company_name} <small style='color: #666; font-size: 0.6em;'>재무 실적 분석</small>", unsafe_allow_html=True)

                        # ==========================================================
                        # Great Tables Styling Logic (Value Horizon Premium)
                        # ==========================================================
                        gt_table = (
                            gt.GT(view_df)
                            .fmt_number(
                                columns=["매출액", "영업이익"],
                                decimals=0,
                                use_seps=True
                            )
                            .fmt_number(
                                columns=["영업이익률"],
                                decimals=2
                            )
                            .fmt(
                                columns=["영업이익률"],
                                fns=lambda x: f"{x:.2f}%"
                            )
                            # 1. 컬럼 레이블 스타일
                            .tab_style(
                                style=[
                                    gt.style.text(weight="600", color="#111111"),
                                    gt.style.fill(color="#ffffff"),
                                ],
                                locations=gt.loc.column_labels()
                            )
                            # 2. 본문 셀 스타일
                            .tab_style(
                                style=[
                                    gt.style.borders(sides="bottom", color="#f0f0f0", weight="1px"),
                                    gt.style.text(color="#1a1a1a")
                                ],
                                locations=gt.loc.body()
                            )
                            # 3. '기간' 컬럼 강조 (Value Horizon Blue Accent)
                            .tab_style(
                                style=[
                                    gt.style.text(weight="600", color="#007aff"),
                                ],
                                locations=gt.loc.body(columns=["기간"])
                            )
                            # 4. 매출/영익 강조
                            .tab_style(
                                style=[
                                    gt.style.text(weight="500")
                                ],
                                locations=gt.loc.body(columns=["매출액", "영업이익"])
                            )
                            .tab_options(
                                table_font_size="13px",
                                table_width="100%",
                                column_labels_font_size="14px",
                                table_border_top_style="none",
                                table_border_bottom_style="none",
                                column_labels_border_top_style="none",
                                column_labels_border_bottom_width="2px",
                                column_labels_border_bottom_color="#111111",
                                table_font_names="Inter",
                                row_striping_include_table_body=True,
                                row_striping_background_color="#f9f9f9"
                            )
                        )

                        # 차트 시각화
                        fig = go.Figure()

                        # Primary Y-axis: 영업이익률 (Smooth Line)
                        fig.add_trace(go.Scatter(
                            x=view_df['기간'],
                            y=view_df['영업이익률'],
                            name='영업이익률 (%)',
                            mode='lines+markers',
                            line=dict(color='#007aff', width=3, shape='spline'),
                            marker=dict(size=8, color='#ffffff', line=dict(color='#007aff', width=2), symbol='circle'),
                            yaxis='y',
                            hovertemplate='<b>%{x}</b><br>영업이익률: %{y:.2f}%<extra></extra>'
                        ))

                        # Secondary Y-axis: 매출액 (Bar)
                        fig.add_trace(go.Bar(
                            x=view_df['기간'],
                            y=view_df['매출액'],
                            name='매출액 (백만)',
                            marker=dict(color='#e5e5ea', opacity=0.8, line=dict(width=0)),
                            yaxis='y2',
                            hovertemplate='<b>%{x}</b><br>매출액: %{y:,.0f}백만<extra></extra>'
                        ))

                        # Secondary Y-axis: 영업이익 (Bar)
                        fig.add_trace(go.Bar(
                            x=view_df['기간'],
                            y=view_df['영업이익'],
                            name='영업이익 (백만)',
                            marker=dict(color='#34c759', opacity=0.8, line=dict(width=0)),
                            yaxis='y2',
                            hovertemplate='<b>%{x}</b><br>영업이익: %{y:,.0f}백만<extra></extra>'
                        ))

                        # 레이아웃 설정
                        fig.update_layout(
                            title=dict(
                                text='📈 핵심 재무지표 추이 분석',
                                font=dict(size=18, color='#111111', family='Inter', weight='700')
                            ),
                            hovermode='x unified',
                            plot_bgcolor='rgba(252,252,252,0.5)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            margin=dict(l=50, r=50, t=100, b=50),
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.05,
                                xanchor="right",
                                x=1,
                                font=dict(size=11, color='#666666')
                            ),
                            xaxis=dict(
                                title='',
                                showgrid=False,
                                tickfont=dict(size=11, color='#8e8e93'),
                                linecolor='#eaeaea'
                            ),
                            yaxis=dict(
                                title='영업이익률 (%)',
                                side='left',
                                showgrid=True,
                                gridcolor='#f2f2f7',
                                ticksuffix='%',
                                tickfont=dict(size=11, color='#007aff'),
                                range=[min(0, view_df['영업이익률'].min() * 1.5), max(view_df['영업이익률'].max() * 1.5, 10)]
                            ),
                            yaxis2=dict(
                                title='금액 (백만)',
                                side='right',
                                overlaying='y',
                                showgrid=False,
                                tickfont=dict(size=11, color='#8e8e93'),
                                tickformat=',.0f'
                            ),
                            bargap=0.35,
                            height=500,
                            font=dict(family='Inter, sans-serif')
                        )

                        actual_max = view_df['영업이익률'].max()
                        actual_min = view_df['영업이익률'].min()

                        fig.add_hline(
                            y=actual_max,
                            line_dash="dot",
                            line_color="#2ECC71",
                            line_width=1.5,
                            annotation_text=f"최대: {actual_max:.1f}%",
                            annotation_position="right",
                            annotation_font=dict(color='#2ECC71', size=10)
                        )

                        fig.add_hline(
                            y=actual_min,
                            line_dash="dot",
                            line_color="#2ECC71",
                            line_width=1.5,
                            annotation_text=f"최소: {actual_min:.1f}%",
                            annotation_position="right",
                            annotation_font=dict(color='#2ECC71', size=10)
                        )

                        # 조건부 레이아웃: 데스크톱 vs 모바일
                        if is_mobile():
                            # 모바일: 기존과 동일하게 위아래로 표시
                            st.html(gt_table.as_raw_html())
                            st.divider()
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            # 데스크톱: 왼쪽(테이블) / 오른쪽(차트) 분할
                            # [수정] vertical_alignment="top" 적용 (테이블과 차트 상단 맞춤)
                            left_col, right_col = st.columns([1, 1], vertical_alignment="top")

                            with left_col:
                                st.html(gt_table.as_raw_html())

                            with right_col:
                                st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    status.update(label="❌ 오류 발생", state="error")
                    st.error(f"처리 중 오류가 발생했습니다: {e}")

elif search_btn and not company_name:
    st.warning("회사명을 입력해주세요.")