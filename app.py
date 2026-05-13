import os
import re
import time
import random
import urllib.parse
import requests
import pandas as pd
from bs4 import BeautifulSoup
import streamlit as st

# ==========================================
# ⚙️ 1. 크롤링 핵심 함수 (기존과 거의 동일)
# ==========================================
BASE_URL = "https://www.data.go.kr"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_soup(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(0.5, 1.2))
            res = requests.get(url, headers=HEADERS, timeout=20)
            res.raise_for_status()
            return BeautifulSoup(res.text, "lxml")
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(random.uniform(2, 4))

def get_total_pages(search_org="", per_page=10):
    base_list_url = "https://www.data.go.kr/tcs/dss/selectDataSetList.do"
    encoded_org = urllib.parse.quote(search_org) if search_org else ""
    list_url = f"{base_list_url}?dType=FILE&sort=updtDt&currentPage=1&perPage={per_page}"
    if search_org: list_url += f"&org={encoded_org}"
    try:
        soup = get_soup(list_url)
        page_numbers = []
        pagination = soup.select_one("nav.pagination, div.pagination, .page")
        if pagination:
            for a in pagination.find_all("a"):
                num_text = re.sub(r'\D', '', a.get_text())
                if num_text: page_numbers.append(int(num_text))
                onclick = a.get("onclick", "")
                nums_in_onclick = re.findall(r'\d+', onclick)
                if nums_in_onclick: page_numbers.extend([int(n) for n in nums_in_onclick])
                href = a.get("href", "")
                nums_in_href = re.findall(r'currentPage=(\d+)', href)
                if nums_in_href: page_numbers.extend([int(n) for n in nums_in_href])
        if page_numbers: return max(page_numbers)
        if soup.select("a[href*='/data/']"): return 1
    except: pass
    return 0

def format_tel_no(tel):
    tel = re.sub(r"\D", "", str(tel))
    if len(tel) == 8: return f"{tel[:4]}-{tel[4:]}"
    if len(tel) == 9: return f"{tel[:2]}-{tel[2:5]}-{tel[5:]}"
    if len(tel) == 10:
        if tel.startswith("02"): return f"{tel[:2]}-{tel[2:6]}-{tel[6:]}"
        return f"{tel[:3]}-{tel[3:6]}-{tel[6:]}"
    if len(tel) == 11: return f"{tel[:3]}-{tel[3:7]}-{tel[7:]}"
    return tel

TARGET_METADATA_KEYS = [
    "파일데이터명", "분류체계", "제공기관", "관리부서명", "관리부서 전화번호",
    "보유근거", "수집방법", "업데이트 주기", "차기 등록 예정일", "매체유형",
    "전체 행", "확장자", "키워드", "데이터 한계", "다운로드(바로가기)",
    "등록일", "수정일", "제공형태", "설명", "기타 유의사항",
    "공간범위", "시간범위", "비용부과유무", "비용부과기준 및 단위", "이용허락범위"
]

METADATA_KEY_MAP = {k.replace(" ", ""): k for k in TARGET_METADATA_KEYS}
METADATA_KEY_MAP["다운로드바로가기"] = "다운로드(바로가기)"
METADATA_KEY_MAP["비용부과기준및단위"] = "비용부과기준 및 단위"
METADATA_KEY_MAP["전화번호"] = "관리부서 전화번호"
METADATA_KEY_MAP["담당자전화번호"] = "관리부서 전화번호"
METADATA_KEY_MAP["연락처"] = "관리부서 전화번호"

ALL_SELECTABLE_COLUMNS = [
    "파일데이터명", "분류체계", "제공기관", "관리부서명", "관리부서 전화번호", "설명", 
    "키워드", "컬럼목록", "전체 행", "확장자", "매체유형", "제공형태", "업데이트 주기", 
    "차기 등록 예정일", "등록일", "수정일", "보유근거", "수집방법", "데이터 한계", 
    "기타 유의사항", "공간범위", "시간범위", "비용부과유무", "비용부과기준 및 단위", 
    "이용허락범위", "다운로드(바로가기)", "상세페이지 URL"
]

def collect_one_detail_page(url):
    metadata = {key: "" for key in TARGET_METADATA_KEYS}
    metadata["상세페이지 URL"] = url
    metadata["컬럼목록"] = ""
    try:
        soup = get_soup(url)
        target_table = next((table for table in soup.select("table") if "파일데이터명" in str(table)), None)
        if target_table:
            for tr in target_table.select("tr"):
                cells = tr.find_all(["th", "td"], recursive=False)
                if not cells: cells = tr.find_all(["th", "td"])
                i = 0
                while i < len(cells) - 1:
                    key = re.sub(r"\s+", "", cells[i].get_text()).replace(":", "").replace("*", "")
                    value = re.sub(r"\s+", " ", cells[i+1].get_text()).strip()
                    mapped_key = METADATA_KEY_MAP.get(key)
                    if mapped_key in metadata: metadata[mapped_key] = value
                    i += 2
                    
        if not metadata.get("관리부서 전화번호"):
            tel_tag = soup.select_one("#telNo, #telNo1")
            if tel_tag:
                tel_text = tel_tag.get_text(strip=True)
                if tel_text: metadata["관리부서 전화번호"] = tel_text

        if not metadata.get("관리부서 전화번호"):
            html_text = str(soup)
            tel_match = re.search(r"var\s+telNo\s*=\s*['\"]([^'\"]+)['\"]", html_text)
            if tel_match: metadata["관리부서 전화번호"] = format_tel_no(tel_match.group(1))

        wrap = soup.select_one("#column-def-table-wrap")
        if wrap:
            for table in wrap.select("table"):
                if "항목명" not in str(table): continue
                trs = table.select("tr")
                if len(trs) > 1:
                    headers = [re.sub(r"\s+", "", th.get_text()) for th in trs[0].select("th, td")]
                    item_idx = next((i for i, h in enumerate(headers) if "항목명" in h), -1)
                    if item_idx != -1:
                        cols = [re.sub(r"\s+", " ", tr.select("th, td")[item_idx].get_text()).strip() for tr in trs[1:] if len(tr.select("th, td")) > item_idx]
                        metadata["컬럼목록"] = ", ".join(list(dict.fromkeys([c for c in cols if c and c not in ["정보시스템명", "DB명", "Table명", "코드"]])))
                        break
    except: pass
    return metadata

# ==========================================
# 🎨 2. 웹 UI 및 로직 (Streamlit 버전)
# ==========================================
st.set_page_config(page_title="공공데이터 크롤러", page_icon="🏢", layout="centered")

# 디테일한 디자인을 위한 CSS
st.markdown("""
    <style>
    .title-spacer {
        margin-bottom: 50px;
    }
    div.stButton > button {
        height: 42px;
    }
    div.stButton > button p {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        gap: 6px !important;
        margin: 0 !important;
    }
    input::placeholder {
        font-size: 14px !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("공공데이터포털 기관별 데이터 크롤링")
st.markdown("<div class='title-spacer'></div>", unsafe_allow_html=True)

# 세션 상태 초기화
if "total_pages" not in st.session_state:
    st.session_state.total_pages = 0
if "target_org" not in st.session_state:
    st.session_state.target_org = ""

st.markdown("**▪&nbsp; 제공기관명 입력** (예: 한국중부발전(주))")
col1, col2 = st.columns([4, 1]) 

with col1:
    org_input = st.text_input(
        "제공기관", 
        label_visibility="collapsed", 
        placeholder="기관명을 입력하면 해당 기관의 모든 공공데이터 목록과 메타데이터를 추출합니다."
    )
    
with col2:
    search_clicked = st.button("검색", use_container_width=True)

# 검색 로직
if search_clicked:
    if not org_input.strip():
        st.warning("제공기관명을 입력해주세요!")
    else:
        with st.spinner(f"'{org_input}' 검색 결과를 확인 중입니다..."):
            total_pages = get_total_pages(search_org=org_input.strip())
            st.session_state.total_pages = total_pages
            st.session_state.target_org = org_input.strip()
            
        if total_pages == 0:
            st.error("❌ 검색 결과가 없습니다. 기관명을 다시 확인해주세요.")
        else:
            st.success(f"✅ 검색 완료! 총 {total_pages}페이지(최대 {total_pages * 10}건)의 데이터가 발견되었습니다.")

# 검색 결과가 있을 때만 아래 UI 표시
if st.session_state.total_pages > 0:
    st.markdown("---")
    
    # 📌 괄호 안의 설명을 빼고 아주 깔끔하게 정리했습니다.
    st.markdown("**▪&nbsp; 추출할 항목 선택**")
    
    options_with_all = ["모두 선택"] + ALL_SELECTABLE_COLUMNS
    
    col_multi, col_btn = st.columns([4, 1])
    
    with col_multi:
        selected_columns = st.multiselect(
            "항목 선택",
            options=options_with_all,
            default=[],
            placeholder="원하는 항목을 골라주세요", # 📌 Choose options를 한글 안내 문구로 변경!
            label_visibility="collapsed"
        )
        
    with col_btn:
        run_clicked = st.button("추출", type="primary", use_container_width=True)
    
    # 추출 버튼 클릭 시 로직
    if run_clicked:
        if not selected_columns:
            st.error("최소 1개 이상의 추출 항목을 선택해주세요!")
        else:
            progress_text = "데이터 추출을 시작합니다..."
            my_bar = st.progress(0, text=progress_text)
            
            try:
                org = st.session_state.target_org
                pages = st.session_state.total_pages
                
                # '모두 선택'이 포함되어 있으면 전체 컬럼을 가져오도록 처리
                if "모두 선택" in selected_columns:
                    target_columns_to_extract = ALL_SELECTABLE_COLUMNS
                else:
                    target_columns_to_extract = selected_columns

                detail_urls = []
                base_list_url = "https://www.data.go.kr/tcs/dss/selectDataSetList.do"
                encoded_org = urllib.parse.quote(org) if org else ""
                
                # 1단계: URL 수집
                for page in range(1, pages + 1):
                    my_bar.progress(int((page / pages) * 30), text=f"URL 수집 중... (페이지 {page}/{pages})")
                    list_url = f"{base_list_url}?dType=FILE&sort=updtDt&currentPage={page}&perPage=10&org={encoded_org}"
                    try:
                        soup = get_soup(list_url)
                        for a in soup.select("a[href]"):
                            href = a.get("href", "")
                            if re.search(r"/data/\d+/fileData\.do", href):
                                detail_urls.append(urllib.parse.urljoin(BASE_URL, href))
                    except: pass
                
                detail_urls = list(dict.fromkeys(detail_urls))
                total_urls = len(detail_urls)
                
                if total_urls == 0:
                    st.error("수집할 URL이 없습니다.")
                else:
                    # 2단계: 상세 데이터 수집
                    rows = []
                    for idx, url in enumerate(detail_urls, start=1):
                        progress_percent = 30 + int((idx / total_urls) * 70)
                        my_bar.progress(progress_percent, text=f"데이터 추출 중... ({idx}/{total_urls} 완료)")
                        rows.append(collect_one_detail_page(url))
                    
                    # 3단계: 데이터프레임 변환
                    result_df = pd.DataFrame(rows)
                    
                    # 추출 대상 컬럼(target_columns_to_extract)을 적용
                    final_cols = [c for c in target_columns_to_extract if c in result_df.columns]
                    result_df = result_df[final_cols]
                    
                    if "관리부서 전화번호" in result_df.columns:
                        result_df["관리부서 전화번호"] = result_df["관리부서 전화번호"].apply(
                            lambda x: f"'{x}" if pd.notnull(x) and str(x).startswith("0") and "-" not in str(x) else x
                        )

                    csv_data = result_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    
                    safe_org_name = org.replace("(", "_").replace(")", "")
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    file_name = f"공공데이터_{safe_org_name}_{timestamp}.csv"
                    
                    my_bar.empty()
                    st.success("✅ 수집 완료! 아래 버튼을 눌러 파일을 다운로드하세요.")
                    
                    st.download_button(
                        label="CSV 파일 다운로드",
                        data=csv_data,
                        file_name=file_name,
                        mime="text/csv",
                    )
            
            except Exception as e:
                st.error(f"🚨 오류 발생: {e}")
