import os
import duckdb
import time
from playwright.sync_api import sync_playwright

# MotherDuck 설정
MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DB_PATH = "md:dart_financials"
APP_URL = "https://search-dart.streamlit.app/"
DEFAULT_PERIOD = "202509" # 기본 기준연월
BATCH_SIZE = 5 # 한 번에 처리할 회사 수

def get_unprocessed_companies():
    """아직 처리되지 않은 회사 목록을 가져옵니다."""
    try:
        conn = duckdb.connect(DB_PATH, config={'motherduck_token': MD_TOKEN})
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
