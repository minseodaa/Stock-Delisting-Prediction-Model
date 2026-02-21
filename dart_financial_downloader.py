import requests
import zipfile
import io
import pandas as pd
from lxml import etree
from tqdm import tqdm
import time
import os

# =====================================================
# 🔑 [1] OpenDART API KEY
# =====================================================
API_KEY = "ec7a408e3c6bb5d9a35ed0df3015fa0c62b2e7ee"
BASE_URL = "https://opendart.fss.or.kr/api"

# =====================================================
# [경로] 스크립트 위치 기준으로 고정
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LISTED_DIR = os.path.join(BASE_DIR, "data", "listed")
DELISTED_DIR = os.path.join(BASE_DIR, "data", "delisted")


# =====================================================
# [2] 전체 법인 코드 다운로드
# =====================================================
def get_all_corp_codes():
    url = f"{BASE_URL}/corpCode.xml"

    response = requests.get(
        url,
        params={"crtfc_key": API_KEY},
        timeout=30
    )
    response.raise_for_status()

    # ZIP 시그니처 검사
    if not response.content.startswith(b"PK"):
        print("❌ CORPCODE 응답이 ZIP이 아님 (OpenDART 서버 오류)")
        print(response.content[:300])
        raise RuntimeError("OpenDART corpCode API 오류")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        xml_data = zip_file.read("CORPCODE.xml")

    root = etree.XML(xml_data)

    corp_list = []
    for corp in root.findall("list"):
        corp_list.append({
            "corp_code": corp.findtext("corp_code"),
            "corp_name": corp.findtext("corp_name"),
            "stock_code": corp.findtext("stock_code")
        })

    return pd.DataFrame(corp_list)




# =====================================================
# [3] 상장 / 상장폐지 판별
# =====================================================
def classify_corp(corp_code):
    url = f"{BASE_URL}/company.json"
    res = requests.get(url, params={
        "crtfc_key": API_KEY,
        "corp_code": corp_code
    }).json()

    if res.get("status") != "000":
        return None

    if not res.get("stock_code"):
        return "DELISTED"
    return "LISTED"


# =====================================================
# [4] 재무제표 수집
# =====================================================
def get_financials(corp_code, year, fs_div):
    url = f"{BASE_URL}/fnlttSinglAcntAll.json"
    res = requests.get(url, params={
        "crtfc_key": API_KEY,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",  # 사업보고서
        "fs_div": fs_div
    }).json()

    if res.get("status") != "000":
        return None

    df = pd.DataFrame(res["list"])
    df["year"] = year
    df["fs_div"] = fs_div
    return df


# =====================================================
# [5] 실행 로직
# =====================================================
def run(start_year=2020, end_year=2022):
    os.makedirs(LISTED_DIR, exist_ok=True)
    os.makedirs(DELISTED_DIR, exist_ok=True)

    corp_code_cache = os.path.join(BASE_DIR, "corp_codes.csv")

    if os.path.exists(corp_code_cache):
        print("✅ corp_codes.csv 캐시 사용")
        corp_df = pd.read_csv(corp_code_cache)
    else:
        print("⬇️ OpenDART에서 corpCode 다운로드 중...")
        corp_df = get_all_corp_codes()
        corp_df.to_csv(corp_code_cache, index=False)
        
        
    corp_df = corp_df[corp_df["stock_code"].notna()]
    print(f"📌 상장사 필터링 후 기업 수: {len(corp_df)}")


    # ▶ 속도 개선 원하면 주석 해제 (상장사만)
    # corp_df = corp_df[corp_df["stock_code"].notna()]

    for _, row in tqdm(corp_df.iterrows(), total=len(corp_df), desc="기업 처리"):
        corp_code = row["corp_code"]
        corp_name = row["corp_name"].replace("/", "_").replace(" ", "")
        file_name = f"{corp_name}_{corp_code}.csv"

        status = classify_corp(corp_code)
        if status is None:
            continue

        yearly_data = []

        for year in range(start_year, end_year + 1):
            # 1️⃣ 연결재무제표 시도
            df = get_financials(corp_code, year, "CFS")

            # 2️⃣ 없으면 별도재무제표 시도
            if df is None:
                df = get_financials(corp_code, year, "OFS")

            if df is not None:
                yearly_data.append(df)

            time.sleep(0.2)  # API 보호

        if not yearly_data:
            continue

        final_df = pd.concat(yearly_data, ignore_index=True)

        save_dir = LISTED_DIR if status == "LISTED" else DELISTED_DIR
        save_path = os.path.join(save_dir, file_name)

        final_df.to_csv(save_path, index=False, encoding="utf-8-sig")


# =====================================================
# [6] 엔트리 포인트
# =====================================================
if __name__ == "__main__":
    run(start_year=2020, end_year=2022)
