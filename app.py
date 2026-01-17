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
from typing import Optional, Dict, List

# ==========================================
# 0. Streamlit 설정 및 상수
# ==========================================
st.set_page_config(
    page_title="DART 재무정보 검색",
    page_icon="📈",
    layout="wide"
)

DB_PATH = "financial_data.duckdb"

# API 키 가져오기 (Streamlit Secrets 우선, 없으면 환경변수)
try:
    API_KEY = st.secrets["DART_API_KEY"]
except (FileNotFoundError, KeyError):
    API_KEY = os.getenv("DART_API_KEY")

# ==========================================
# 1. Database 초기화
# ==========================================
def init_db():
    conn = duckdb.connect(DB_PATH)
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
    conn.close()

# 앱 실행 시 DB 초기화
init_db()

# ==========================================
# 2. DART 고유번호(Corp Code) 관리 (Cached)
# ==========================================

@st.cache_data(ttl=3600*24)  # 24시간 캐시
def get_company_codes(api_key: str) -> Optional[Dict[str, str]]:
    """
    Open DART에서 고유번호(8자리)를 받아와 딕셔너리로 반환합니다.
    Streamlit Cache를 사용하여 매번 다운로드하지 않도록 최적화합니다.
    """
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
                        if code and name:
                            data_list.append({'corp_name': name, 'corp_code': code})

            if data_list:
                df = pd.DataFrame(data_list)
                return df.set_index('corp_name')['corp_code'].to_dict()
        return None
    except Exception as e:
        st.error(f"고유번호 다운로드 실패: {e}")
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
        print(f"DB 저장 실패: {e}")

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
    return adjust_q4_values(filtered)

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
# 5. UI Layout
# ==========================================

st.title("📊 DART 재무정보 조회")
st.markdown("회사명과 기준 연월을 입력하면 최근 4년치 **매출액, 영업이익, 영업이익률** 추이를 보여줍니다.")

if not API_KEY:
    st.error("🚨 DART API Key가 설정되지 않았습니다. Streamlit Secrets에 `DART_API_KEY`를 설정해주세요.")
    st.stop()

with st.sidebar:
    st.header("검색 설정")
    company_name = st.text_input("회사명", placeholder="예: 삼성전자")
    year_month = st.text_input("기준 연월 (YYYYMM)", value="202509", placeholder="202509")
    search_btn = st.button("조회하기", type="primary", use_container_width=True)
    st.markdown("---")
    st.caption("Data source: Open DART API")

if search_btn and company_name and year_month:
    if not year_month.isdigit() or len(year_month) != 6:
        st.error("기준 연월은 YYYYMM 형식의 6자리 숫자여야 합니다.")
    else:
        with st.status("데이터를 조회하고 있습니다...", expanded=True) as status:
            st.write("🏢 기업 고유번호 검색 중...")
            corp_code = search_company_code(API_KEY, company_name)
            
            if not corp_code:
                status.update(label="❌ 회사를 찾을 수 없습니다.", state="error")
                st.error(f"'{company_name}' 회사를 찾을 수 없습니다.")
            else:
                st.write(f"✅ 고유번호 확인: {corp_code}")
                st.write("📥 재무 데이터 수집 및 분석 중...")
                
                start_time = time.time()
                try:
                    raw_df = collect_financials(API_KEY, corp_code, int(year_month))
                    
                    if raw_df.empty:
                        status.update(label="❌ 데이터 없음", state="error")
                        st.warning("해당 기간의 재무 데이터를 찾을 수 없습니다.")
                    else:
                        view_df = process_dataframe_for_view(raw_df)
                        elapsed = time.time() - start_time
                        
                        status.update(label=f"✅ 조회 완료! ({elapsed:.2f}초)", state="complete")
                        
                        st.subheader(f"{company_name} 재무 추이")
                        st.dataframe(
                            view_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "기간": st.column_config.TextColumn("기간", width="medium"),
                                "매출액": st.column_config.NumberColumn(
                                    "매출액 (백만원)", format="%d"
                                ),
                                "영업이익": st.column_config.NumberColumn(
                                    "영업이익 (백만원)", format="%d"
                                ),
                                "영업이익률": st.column_config.NumberColumn(
                                    "영업이익률 (%)", format="%.2f %%"
                                ),
                            }
                        )
                        
                        # 차트 시각화 (보너스 기능)
                        st.divider()
                        st.subheader("📈 Trend Chart")

                        # Plotly를 사용하여 차트 생성 (영업이익률: primary y-axis, 매출액/영업이익: secondary y-axis)
                        fig = go.Figure()

                        # Primary Y-axis: 영업이익률 (Line)
                        fig.add_trace(go.Scatter(
                            x=view_df['기간'],
                            y=view_df['영업이익률'],
                            name='영업이익률 (%)',
                            mode='lines+markers',
                            line=dict(color='green', width=3),
                            marker=dict(size=8),
                            yaxis='y'
                        ))

                        # Secondary Y-axis: 매출액 (Bar)
                        fig.add_trace(go.Bar(
                            x=view_df['기간'],
                            y=view_df['매출액'],
                            name='매출액 (백만원)',
                            marker=dict(color='royalblue'),
                            yaxis='y2'
                        ))

                        # Secondary Y-axis: 영업이익 (Bar)
                        fig.add_trace(go.Bar(
                            x=view_df['기간'],
                            y=view_df['영업이익'],
                            name='영업이익 (백만원)',
                            marker=dict(color='firebrick'),
                            yaxis='y2'
                        ))

                        # 레이아웃 설정
                        fig.update_layout(
                            title='재무 추이 (영업이익률, 매출액, 영업이익)',
                            xaxis=dict(title='기간'),
                            yaxis=dict(
                                title='영업이익률 (%)',
                                tickfont=dict(color='green'),
                                side='left'
                            ),
                            yaxis2=dict(
                                title='금액 (백만원)',
                                tickfont=dict(color='royalblue'),
                                overlaying='y',
                                side='right'
                            ),
                            hovermode='x unified',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            ),
                            barmode='group',
                            margin=dict(l=50, r=50, b=50, t=80, pad=4),
                            height=500
                        )

                        st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    status.update(label="❌ 오류 발생", state="error")
                    st.error(f"처리 중 오류가 발생했습니다: {e}")
                    # 디버깅용: st.exception(e)

elif search_btn and not company_name:
    st.warning("회사명을 입력해주세요.")
