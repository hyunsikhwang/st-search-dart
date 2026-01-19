import os
import duckdb
import time
import pandas as pd
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
                        stock = corp.findtext('stock_code', '').strip()
                        # 주식 코드가 있는(상장사) 경우에만 추가
                        if code and name and stock:
                            data_list.append((code, name, stock))
            
            if data_list:
                print(f"[Database] Preparing to insert {len(data_list)} records...", flush=True)
                df = pd.DataFrame(data_list, columns=['corp_code', 'corp_name', 'stock_code'])
                
                conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
                conn.execute("CREATE DATABASE IF NOT EXISTS dart_financials")
                conn.execute("USE dart_financials")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS corp_codes (
                        corp_code VARCHAR PRIMARY KEY,
                        corp_name VARCHAR,
                        stock_code VARCHAR,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                # [수정] 스키마 변경 시 컬럼 추가를 위해 처리
                try:
                    conn.execute("ALTER TABLE corp_codes ADD COLUMN IF NOT EXISTS stock_code VARCHAR")
                except:
                    pass

                print("[Database] Executing bulk insert (INSERT OR REPLACE)...", flush=True)
                conn.execute("INSERT OR REPLACE INTO corp_codes (corp_code, corp_name, stock_code) SELECT corp_code, corp_name, stock_code FROM df")
                conn.close()
                print(f"[Database] Successfully synced {len(data_list)} corp codes.", flush=True)
                return True
        return False
    except Exception as e:
        print(f"Failed to sync corp codes: {e}")
        return False

def get_unprocessed_companies():
    """아직 처리되지 않은 회사 목록을 가져옵니다."""
    try:
        print(f"[Database] Connecting to MotherDuck (Path: {DB_PATH})...", flush=True)
        conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
        print("[Database] Connected. Initializing tables...", flush=True)
        conn.execute("CREATE DATABASE IF NOT EXISTS dart_financials")
        conn.execute("USE dart_financials")
        
        # 테이블 존재 확인 및 생성
        conn.execute("""
            CREATE TABLE IF NOT EXISTS corp_codes (
                corp_code VARCHAR PRIMARY KEY,
                corp_name VARCHAR,
                stock_code VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # [수정] 스키마 변경 시 컬럼 추가를 위해 처리
        try:
            conn.execute("ALTER TABLE corp_codes ADD COLUMN IF NOT EXISTS stock_code VARCHAR")
        except:
            pass
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
        # [수정] 스키마 변경 시 컬럼 추가를 위해 처리
        try:
            conn.execute("ALTER TABLE processing_status ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'SUCCESS'")
        except:
            pass
        
        # 데이터가 있는지 확인
        count = conn.execute("SELECT count(*) FROM corp_codes").fetchone()[0]
        print(f"[Database] Current corp_codes count: {count}", flush=True)
        if count == 0:
            conn.close()
            if sync_corp_codes():
                return get_unprocessed_companies() # 재시도
            return []

        print("[Database] Fetching unprocessed companies...", flush=True)
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
        print(f"[Database Error] {e}", flush=True)
        return []

def run_automation():
    print("--- Starting Automation Script ---", flush=True)
    companies = get_unprocessed_companies()
    if not companies:
        print("[Status] No unprocessed companies found. Everything is up to date.", flush=True)
        return

    print(f"[Status] Found {len(companies)} companies to process.", flush=True)

    with sync_playwright() as p:
        print("[Playwright] Launching browser...", flush=True)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1280, 'height': 800})
        page = context.new_page()

        for i, company in enumerate(companies):
            name = company['corp_name']
            code = company['corp_code']
            print(f"\n[{i+1}/{len(companies)}] Processing: {name} ({code})", flush=True)

            try:
                print(f"  - Navigating to {APP_URL}...", flush=True)
                page.goto(APP_URL, wait_until="networkidle", timeout=60000)
                
                print("  - Locating Streamlit iframe...", flush=True)
                # Streamlit Cloud는 보통 메인 컨텐츠를 iframe 안에 둠
                main_frame = page.frame_locator('iframe[title="streamlitApp"]')
                
                print("  - Waiting for Streamlit UI to load inside iframe...", flush=True)
                # iframe 내부에서 인풋 박스가 나타날 때까지 대기
                input_selector = 'input[aria-label="회사명"]'
                main_frame.locator(input_selector).wait_for(state="visible", timeout=60000)
                
                print(f"  - Filling company name: {name}", flush=True)
                main_frame.get_by_label("회사명").fill(name)
                
                print(f"  - Filling period: {DEFAULT_PERIOD}", flush=True)
                main_frame.get_by_label("기준 연월 (YYYYMM)").fill(DEFAULT_PERIOD)
                
                print("  - Clicking '조회하기' button...", flush=True)
                main_frame.get_by_role("button", name="조회하기").click()
                
                print("  - Waiting for data collection to complete...", flush=True)
                # iframe 내부의 텍스트 변화를 감지해야 함
                # wait_for_function은 page 단위이므로, 텍스트가 전체 페이지에 나타나는지 확인
                page.wait_for_function("""
                    () => {
                        const texts = document.body.innerText;
                        return texts.includes("조회 완료") || texts.includes("❌") || texts.includes("데이터를 조회하고 있습니다");
                    }
                """, timeout=60000)

                # 실제 완료까지 조금 더 대기
                page.wait_for_function("""
                    () => {
                        const texts = document.body.innerText;
                        return texts.includes("조회 완료") || texts.includes("❌");
                    }
                """, timeout=60000)
                
                page_content = page.content()
                if "조회 완료" in page_content:
                    print(f"  - [Success] Successfully processed {name}", flush=True)
                elif "❌" in page_content:
                    print(f"  - [Warning] App reported an error for {name}. Data might be missing.", flush=True)
                else:
                    print(f"  - [Error] Could not confirm completion for {name}", flush=True)
                
                # 서버 부하 방지를 위해 잠시 대기
                print("  - Cooling down for 5 seconds...", flush=True)
                time.sleep(5)
                
            except Exception as e:
                print(f"  - [Critical Error] Failed to process {name}: {e}", flush=True)
                # 실패 상황 캡처를 위해 에러 로그 출력 시점의 스크린샷은 action artifact에는 안남지만 로컬에선 유용
                # page.screenshot(path=f"error_{code}.png")

        print("\n[Playwright] Closing browser...", flush=True)
        browser.close()
    print("--- Automation Task Finished ---", flush=True)

if __name__ == "__main__":
    print("--- Script Entry Point ---", flush=True)
    if not MD_TOKEN:
        print("MOTHERDUCK_TOKEN is not set.", flush=True)
    else:
        run_automation()
