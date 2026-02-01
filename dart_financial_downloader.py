import requests
import zipfile
import io
import pandas as pd
from lxml import etree
from tqdm import tqdm
import time
import os

# =====================================================
# 🔑 [1] OpenDART API KEY 입력 위치 (여기만 수정)
# =====================================================
API_KEY = "여기에_본인_API_KEY"
# 예)
# API_KEY = "1234567890abcdef1234567890abcdef"

BASE_URL = "https://opendart.fss.or.kr/api"


# =====================================================
# [2] 전체 법인 코드(corp_code) 다운로드
# =====================================================
def get_all_corp_codes():
    url = f"{BASE_URL}/corpCode.xml"
    response = requests.get(url, params={"crtfc_key": API_KEY})
    response.raise_for_status()

    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
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

    # stock_code가 없으면 상장폐지로 판단
    if not res.get("stock_code"):
        return "DELISTED"
    return "LISTED"


# =====================================================
# [4] 재무제표 수집
# =====================================================
def get_financials(corp_code, year, fs_div="CFS"):
    """
    fs_div:
      - CFS : 연결재무제표
      - OFS : 별도재무제표
    """
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
    return df


# =====================================================
# [5] 전체 실행 로직
# =====================================================
def run(start_year=2012, end_year=2024):
    os.makedirs("data/listed", exist_ok=True)
    os.makedirs("data/delisted", exist_ok=True)

    corp_df = get_all_corp_codes()

    for _, row in tqdm(corp_df.iterrows(), total=len(corp_df)):
        corp_code = row["corp_code"]
        corp_name = row["corp_name"].replace("/", "_")

        status = classify_corp(corp_code)
        if status is None:
            continue

        yearly_data = []

        for year in range(start_year, end_year + 1):
            df = get_financials(corp_code, year)
            if df is not None:
                yearly_data.append(df)

            # OpenDART 호출 제한 보호
            time.sleep(0.2)

        if not yearly_data:
            continue

        final_df = pd.concat(yearly_data, ignore_index=True)

        save_dir = "listed" if status == "LISTED" else "delisted"
        final_df.to_csv(
            f"data/{save_dir}/{corp_name}.csv",
            index=False,
            encoding="utf-8-sig"
        )


# =====================================================
# [6] 실행 엔트리 포인트
# =====================================================
if __name__ == "__main__":
    run(start_year=2012, end_year=2024)
