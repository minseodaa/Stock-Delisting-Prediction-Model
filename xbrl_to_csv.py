from lxml import etree
import pandas as pd

# ==========================
# 경로 설정
# ==========================
XBRL_PATH = r"C:\사업보고서크롤링\00126380_2011-04-30.xbrl"

# ==========================
# XBRL 로드
# ==========================
tree = etree.parse(XBRL_PATH)
root = tree.getroot()

ns = root.nsmap

records = []

# ==========================
# 모든 Fact 추출
# ==========================
for elem in root.iter():
    if not isinstance(elem.tag, str):
        continue

    if ":" not in elem.tag:
        continue

    tag = elem.tag.split("}")[-1]

    context = elem.attrib.get("contextRef")
    unit = elem.attrib.get("unitRef")

    try:
        value = float(elem.text.replace(",", ""))
    except:
        continue

    records.append({
        "account": tag,
        "value": value,
        "context": context,
        "unit": unit
    })

df = pd.DataFrame(records)

# ==========================
# 재무제표 분류 키워드
# ==========================
BALANCE_KEYWORDS = [
    "Assets", "Liabilities", "Equity"
]

INCOME_KEYWORDS = [
    "Revenue", "Profit", "Loss", "OperatingIncome"
]

CASHFLOW_KEYWORDS = [
    "CashFlows", "NetCash", "CashAndCashEquivalents"
]

# ==========================
# 분리
# ==========================
balance_df = df[df["account"].str.contains("|".join(BALANCE_KEYWORDS), case=False)]
income_df = df[df["account"].str.contains("|".join(INCOME_KEYWORDS), case=False)]
cashflow_df = df[df["account"].str.contains("|".join(CASHFLOW_KEYWORDS), case=False)]

# ==========================
# 저장
# ==========================
balance_df.to_csv("balance_sheet.csv", index=False, encoding="utf-8-sig")
income_df.to_csv("income_statement.csv", index=False, encoding="utf-8-sig")
cashflow_df.to_csv("cashflow_statement.csv", index=False, encoding="utf-8-sig")

print("✅ 재무제표 CSV 생성 완료")
