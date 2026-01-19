import os
import duckdb
import time
from playwright.sync_api import sync_playwright

# 설정
MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
API_KEY = os.getenv("DART_API_KEY")
DB_PATH = "md:"
APP_URL = "https://search-dart.streamlit.app/"
DEFAULT_PERIOD = "202509" # 기본 기준연월
BATCH_SIZE = 5 # 한 번에 처리할 회사 수

import requests
import zipfile
import io
import xml.etree.ElementTree as ET

def sync_corp_codes():
    """DART API에서 회사 코드를 가져와 DB에 저장합니다."""
    if not API_KEY:
        print("DART_API_KEY is not set. Skipping sync.")
        return False
    
    print("Syncing corp codes from DART API...")
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {'crtfc_key': API_KEY}
    
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
                            data_list.append((code, name))
            
            if data_list:
                conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
                conn.execute("CREATE DATABASE IF NOT EXISTS dart_financials")
                conn.execute("USE dart_financials")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS corp_codes (
                        corp_code VARCHAR PRIMARY KEY,
                        corp_name VARCHAR,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.executemany("""
                    INSERT OR REPLACE INTO corp_codes (corp_code, corp_name, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, data_list)
                conn.close()
                print(f"Successfully synced {len(data_list)} corp codes.")
                return True
        return False
    except Exception as e:
        print(f"Failed to sync corp codes: {e}")
        return False

def get_unprocessed_companies():
    """아직 처리되지 않은 회사 목록을 가져옵니다."""
    try:
        conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
        conn.execute("CREATE DATABASE IF NOT EXISTS dart_financials")
        conn.execute("USE dart_financials")
        
        # 테이블 존재 확인 및 생성
        conn.execute("""
            CREATE TABLE IF NOT EXISTS corp_codes (
                corp_code VARCHAR PRIMARY KEY,
                corp_name VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_status (
                corp_code VARCHAR,
                corp_name VARCHAR,
                last_base_period VARCHAR,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (corp_code)
            )
        """)
        
        # 데이터가 있는지 확인
        count = conn.execute("SELECT count(*) FROM corp_codes").fetchone()[0]
        if count == 0:
            conn.close()
            if sync_corp_codes():
                return get_unprocessed_companies() # 재시도
            return []

        query = """
            SELECT c.corp_name, c.corp_code 
            FROM corp_codes c
            LEFT JOIN processing_status p ON c.corp_code = p.corp_code
            WHERE p.corp_code IS NULL
            ORDER BY c.corp_code ASC
            LIMIT ?
        """
        df = conn.execute(query, [BATCH_SIZE]).df()
        conn.close()
        return df.to_dict('records')
    except Exception as e:
        print(f"Error fetching companies: {e}")
        return []

def run_automation():
    companies = get_unprocessed_companies()
    if not companies:
        print("No unprocessed companies found.")
        return

    print(f"Processing {len(companies)} companies...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for company in companies:
            name = company['corp_name']
            code = company['corp_code']
            print(f"Searching for: {name} ({code})")

            try:
                page.goto(APP_URL)
                # Streamlit 로딩 대기
                page.wait_for_selector('div[data-testid="stTextInput"]', timeout=30000)
                
                # 입력 필드 찾기 및 입력
                page.get_by_label("회사명").fill(name)
                
                # 기준 연월 입력 (기본값 202509)
                page.get_by_label("기준 연월 (YYYYMM)").fill(DEFAULT_PERIOD)
                
                # 조회하기 버튼 클릭
                page.get_by_role("button", name="조회하기").click()
                
                # 결과가 나올 때까지 대기 (상태 메시지 변화 확인)
                # "✅ 조회 완료!" 또는 "❌" 가 포함된 텍스트가 나타날 때까지 대기
                # st.status 내부의 텍스트를 감지함
                page.wait_for_function("""
                    () => {
                        const texts = document.body.innerText;
                        return texts.includes("조회 완료") || texts.includes("❌");
                    }
                """, timeout=90000)
                
                if "조회 완료" in page.content():
                    print(f"Successfully processed {name}")
                else:
                    print(f"Failed to process {name}: Data not found or error occurred")
                
                # 서버 부하 방지를 위해 잠시 대기
                time.sleep(5)
                
            except Exception as e:
                print(f"Failed to process {name}: {e}")

        browser.close()

if __name__ == "__main__":
    if not MD_TOKEN:
        print("MOTHERDUCK_TOKEN is not set.")
    else:
        run_automation()
