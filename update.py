#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深圳楼市数据更新脚本 v3
修复:
1. 住宅网签 = 预售网签 + 现售网签 (住宅)
2. 去化周期 = 库存套数 / 过去12个月月均成交套数 (预售+现售合计)
   - 官方API只返回上月单月数据, 需从第三方来源补全12个月历史
   - 预置2025-03至今的月度数据(来自乐有家/中原/国策等)
   - 后续每次运行自动累积最新月度数据
3. 新房/二手房成交均价数据 (第三方来源)
"""

import json
import urllib.parse
import time
import subprocess
import os
import re
from datetime import datetime

BASE = "https://fdc.zjj.sz.gov.cn/szfdccommon"
ZONES = ["全市","罗湖","福田","南山","盐田","宝安","龙岗","龙华","光明","坪山","大鹏","深汕"]
ZONE_ONLY = [z for z in ZONES if z != "全市"]
DIR = "/Users/renzheng/WorkBuddy/20260320134026"

# ============================================================
# 预置一手住宅月度成交历史数据 (预售+现售, 来自乐有家/中原/国策等公开数据)
# 去化周期 = 库存 / 过去12个月月均成交套数
# ============================================================
PRESET_MONTHLY_DEALS = {
    # 月份: {ysf: 一手住宅成交套数(预售+现售), esf: 二手住宅成交套数, src: 数据来源}
    "2025-03": {"ysf": 4161, "esf": 6229, "src": "乐有家/知乎"},
    "2025-04": {"ysf": 3696, "esf": 5895, "src": "乐有家/知乎"},
    "2025-05": {"ysf": 3275, "esf": 5267, "src": "央广/乐有家"},
    "2025-06": {"ysf": 3275, "esf": 4656, "src": "央广/乐有家"},
    "2025-07": {"ysf": 2660, "esf": 4656, "src": "乐有家/中原"},
    "2025-08": {"ysf": 2151, "esf": 4175, "src": "乐有家"},
    "2025-09": {"ysf": 1712, "esf": 4028, "src": "国策评估"},
    "2025-10": {"ysf": 1690, "esf": 4042, "src": "国策评估"},
    "2025-11": {"ysf": 1703, "esf": 4196, "src": "国策评估"},
    "2025-12": {"ysf": 3470, "esf": 6613, "src": "深圳商报"},
    "2026-01": {"ysf": 2579, "esf": 5281, "src": "乐有家"},
}


def get(path, params=None):
    url = BASE + path + (("?" + urllib.parse.urlencode(params)) if params else "")
    try:
        r = subprocess.run(
            ['curl','-sk','--max-time','20',
             '-H','Accept: application/json',
             '-H','Referer: https://fdc.zjj.sz.gov.cn/',
             url],
            capture_output=True, text=True, timeout=25
        )
        if r.returncode == 0 and r.stdout.strip().startswith('{'):
            return json.loads(r.stdout)
    except: pass
    return None


def sl(d, k="list"):
    try:
        lst = d.get("data", d) if d else {}
        return lst.get(k) or [] if isinstance(lst, dict) else []
    except: return []


def get_residential(items):
    """从列表中提取住宅类型数据 (兼容多种字段名和大小写)"""
    if not items:
        return {}
    for i in items:
        v = (i.get("useage") or i.get("useAge") or
             i.get("usage") or i.get("reportcatalog") or "")
        if v == "住宅":
            return i
    return {}


def load_monthly_history():
    """加载月度历史成交数据"""
    history_path = os.path.join(DIR, "sz_monthly_history.json")
    history = {}
    # 先加载预置数据
    for k, v in PRESET_MONTHLY_DEALS.items():
        history[k] = dict(v)
    # 再加载保存的增量数据 (会覆盖预置的)
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            if isinstance(saved, list):
                for item in saved:
                    if isinstance(item, dict) and "date" in item:
                        history[item["date"]] = item
            elif isinstance(saved, dict):
                for k, v in saved.items():
                    if v.get("ysf", 0) > 0:  # 只覆盖有有效数据的
                        history[k] = v
        except:
            pass
    return history


def save_monthly_history(history):
    """保存月度历史成交数据 (仅保存API自动采集的, 不覆盖预置的)"""
    history_path = os.path.join(DIR, "sz_monthly_history.json")
    # 只保存非预置的增量数据
    incremental = {}
    for k, v in history.items():
        if k not in PRESET_MONTHLY_DEALS:
            incremental[k] = v
    with open(history_path, 'w', encoding='utf-8') as f:
        json.dump(incremental, f, ensure_ascii=False, indent=2)


def calc_avg_12m(history, month_str):
    """计算指定月份之前12个月的月均一手住宅成交套数 (预售+现售合计)"""
    # 从month_str往前推12个月
    parts = month_str.split('-')
    year, month = int(parts[0]), int(parts[1])
    months_12 = []
    for i in range(12):
        m = month - i
        y = year
        while m <= 0:
            m += 12
            y -= 1
        key = f"{y}-{m:02d}"
        if key in history and history[key].get("ysf", 0) > 0:
            months_12.append(history[key]["ysf"])
    if len(months_12) == 0:
        return 0, 0  # avg, count
    avg = round(sum(months_12) / len(months_12), 1)
    return avg, len(months_12)


def fetch():
    print(f"采集数据 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    D = {}

    # ========== 0. 加载历史数据 ==========
    history = load_monthly_history()
    print(f"  历史数据: {len(history)} 个月")

    # ========== 1. 核心KPI ==========
    print("  [1/9] 核心指标...")

    # 1a. 认购数据 (ysfcjgs1)
    r1 = get("/ysfcjxxnew/ysfcjgs1", {"zone": "全市"})
    rg = get_residential(sl(r1))
    D["dataDate"] = r1.get("data", {}).get("xmlDateDay", "") if r1 else ""

    # 1b. 网签数据 = 预售网签 + 现售网签 (住宅)
    # 预售成交当日 (getXsCjxxGsData) - 每日更新, xmlDateDay为数据日期
    r_xs = get("/ysfcjxxnew/getXsCjxxGsData")
    xs_date = ""
    if r_xs and r_xs.get("status") == 200:
        xs_date = r_xs.get("data", {}).get("xmlDateDay", "")
    xs_ts_total = 0
    xs_mj_total = 0
    if r_xs and r_xs.get("status") == 200:
        for item in r_xs.get("data", {}).get("dataTs", []):
            xs_ts_total += item.get("value", 0)
        for item in r_xs.get("data", {}).get("dataMj", []):
            xs_mj_total += item.get("value", 0)

    # 现售成交当日 (getYsfCjxxGsData) - 每日更新
    r_ys = get("/ysfcjxxnew/getYsfCjxxGsData")
    ys_date = ""
    if r_ys and r_ys.get("status") == 200:
        ys_date = r_ys.get("data", {}).get("xmlDateDay", "")
    ys_ts_total = 0
    ys_mj_total = 0
    if r_ys and r_ys.get("status") == 200:
        for item in r_ys.get("data", {}).get("dataTs", []):
            ys_ts_total += item.get("value", 0)
        for item in r_ys.get("data", {}).get("dataMj", []):
            ys_mj_total += item.get("value", 0)

    wq_ts = xs_ts_total + ys_ts_total
    wq_mj = xs_mj_total + ys_mj_total
    # 使用预售网签的日期作为数据日期(日更新)
    data_date = xs_date or ys_date or D.get("dataDate", "")
    D["dataDate"] = data_date
    print(f"    数据日期: {data_date}")
    print(f"    预售网签: {xs_ts_total}套, 现售网签: {ys_ts_total}套, 合计: {wq_ts}套")

    # 1c. 二手房当日成交 (esfcjgsDay) - 每日更新
    r3 = get("/esfCjxxNew/esfcjgsDay", {"zone": "全市"})
    esf = get_residential(sl(r3))
    esf_date = ""
    if r3 and r3.get("status") == 200:
        esf_date = r3.get("data", {}).get("xmlDateDay", "")


    # 1d. 库存
    r4 = get("/ysfcjxxnew/ysfcjgs2ForMonth", {"zone": "全市"})
    inv_all = get_residential(sl(r4))
    kc_ts = inv_all.get("marketableCount", 0)
    kc_mj = inv_all.get("marketableArea", 0)
    last_deal = inv_all.get("dealCount", 0)  # 上月一手住宅成交

    D["kpi"] = {
        "rg_ts": rg.get("rgts", 0), "rg_mj": rg.get("rgarea", 0),
        "wq_ts": wq_ts, "wq_mj": wq_mj,
        "wq_xs_ts": xs_ts_total, "wq_ys_ts": ys_ts_total,
        "esf_ts": esf.get("contractCount", 0), "esf_mj": esf.get("buildingArea", 0),
        "kc_ts": kc_ts, "kc_mj": kc_mj,
        "last_deal": last_deal,
        "dataDate": data_date,
        "wqDate": xs_date,
        "esfDate": esf_date,
        "kcDate": r4.get("data", {}).get("xmlDateMonth", "") if r4 else "",
    }

    # ========== 2. 分区认购 ==========
    print("  [2/9] 分区认购...")
    rg_d = []
    for z in ZONE_ONLY:
        r = get("/ysfcjxxnew/ysfcjgs1", {"zone": z})
        item = get_residential(sl(r))
        rg_d.append({"name": z, "value": item.get("rgts", 0)})
        time.sleep(0.15)
    D["rg_district"] = rg_d

    # ========== 3. 分区网签 (预售+现售) ==========
    print("  [3/9] 分区网签(预售+现售)...")
    wq_d = {}
    if r_xs and r_xs.get("status") == 200:
        for item in r_xs.get("data", {}).get("dataTs", []):
            wq_d[item["name"]] = wq_d.get(item["name"], 0) + item.get("value", 0)
    if r_ys and r_ys.get("status") == 200:
        for item in r_ys.get("data", {}).get("dataTs", []):
            wq_d[item["name"]] = wq_d.get(item["name"], 0) + item.get("value", 0)
    D["wq_district"] = [{"name": k, "value": v} for k, v in wq_d.items()]

    # ========== 4. 库存数据 (分区) ==========
    print("  [4/9] 库存数据...")
    deho = []
    zone_inv = {}  # zone -> {inv, deal}
    for z in ZONES:
        r = get("/ysfcjxxnew/ysfcjgs2ForMonth", {"zone": z})
        inv = get_residential(sl(r))
        ic_z = inv.get("marketableCount", 0)
        dc_z = inv.get("dealCount", 0)
        zone_inv[z] = {"inv": ic_z, "deal": dc_z}
        deho.append({
            "zone": z, "inv": ic_z,
            "area": round(inv.get("marketableArea", 0) / 10000, 2),
            "deal": dc_z, "rg": 0,
            "cycle": -1,
        })
        time.sleep(0.15)
    D["deho"] = deho

    # ========== 5. 去化周期计算 (核心修复) ==========
    print("  [5/9] 去化周期计算...")
    # 去化周期 = 库存套数 / 过去12个月月均成交套数
    # "过去12个月月均成交套数" = 一手住宅预售+现售合计的12个月平均

    # 当前参考月份: 用API返回的上月数据来确定当前处于哪个月
    # xmlDateMonth 或 last_deal 的月份
    current_ref = r4.get("data", {}).get("xmlDateMonth", "") if r4 else ""
    # 解析月份, 格式如 "2026年2月" 或直接用当前月份
    now_month = datetime.now().strftime('%Y-%m')
    # API的 dealCount 是上一个月的数据
    if current_ref:
        # 解析 "2026年2月" -> "2026-02"
        m = re.search(r'(\d{4})年(\d{1,2})月', current_ref)
        if m:
            ref_month = f"{m.group(1)}-{int(m.group(2)):02d}"
        else:
            ref_month = now_month
    else:
        ref_month = now_month

    # 用API返回的上月成交数据更新历史
    # 注意: ysfcjgs2ForMonth.dealCount = 上月预售住宅成交 (不含现售!)
    # 需要: dealCount(预售) + getLastMonthXsInfoByZone的现售部分 = 一手合计
    # 但getLastMonthXsInfoByZone只有预售的wqts
    # 实际上 xsfMsg 提到"现售...商品住宅成交547套" 但那是文本, 无法API解析
    # 策略: 如果ref_month不在预置历史中, 用dealCount作为预估
    # 如果在预置历史中, 保持预置数据(更准确)
    if ref_month not in history and last_deal > 0:
        # 无预置数据, 用API返回的预售数据(会低估, 因为缺现售)
        history[ref_month] = {"ysf": last_deal, "esf": 0, "src": "API_预售only"}

    # 也从上月二手房月度接口获取数据
    r_esf_m = get("/esfCjxxNew/esfcjgsMonth", {"zone": "全市"})
    esf_m_item = get_residential(sl(r_esf_m))
    esf_m_ts = esf_m_item.get("contractCount", 0)
    if ref_month in history:
        history[ref_month]["esf"] = esf_m_ts

    # 计算去化周期
    avg_12m, count_12m = calc_avg_12m(history, ref_month)
    print(f"    参考月份: {ref_month}")
    print(f"    有效历史月数: {count_12m}/12")
    print(f"    过去{count_12m}个月月均一手住宅成交(预售+现售): {avg_12m}套")
    print(f"    库存: {kc_ts}套")
    if avg_12m > 0:
        cycle_city = round(kc_ts / avg_12m, 1)
        print(f"    去化周期: {cycle_city}月")
    else:
        cycle_city = -1
        print(f"    去化周期: 无法计算(无历史数据)")

    # 更新全市去化
    for d in D["deho"]:
        if d["zone"] == "全市":
            d["avg12m"] = avg_12m
            d["cycle"] = cycle_city
            d["history_months"] = count_12m
            D["kpi"]["dh_month"] = cycle_city
            D["kpi"]["avg12m"] = avg_12m
            D["kpi"]["history_months"] = count_12m

    # 分区去化: 分区上月成交 / 全市上月成交 = 分区占比
    # 分区月均 = 全市12月均 * 分区占比
    city_last_deal = zone_inv.get("全市", {}).get("deal", 0)
    for d in D["deho"]:
        if d["zone"] != "全市":
            z_deal = d["deal"]
            if city_last_deal > 0 and avg_12m > 0 and z_deal >= 0:
                ratio = z_deal / city_last_deal
                z_avg = round(avg_12m * ratio, 1)
                d["avg12m"] = z_avg
                d["cycle"] = round(d["inv"] / z_avg, 1) if z_avg > 0 else -1
            else:
                d["avg12m"] = d["deal"]
                d["cycle"] = round(d["inv"] / max(d["deal"], 1), 1) if d["deal"] > 0 else -1

    # 保存历史
    save_monthly_history(history)

    # ========== 6. 新批准预售 ==========
    print("  [6/9] 新批准预售...")
    rx = get("/ysfcjxxnew/xssList")
    D["xss"] = [{"district": x.get("district", ""), "spzfts": x.get("spzfts", 0), "spfts": x.get("spfts", 0)}
                for x in (rx.get("data", []) if rx and rx.get("status") == 200 else [])]

    # ========== 7. 价格指数 ==========
    print("  [7/9] 价格指数...")
    D["priceNew"], D["priceEsf"] = [], []
    for tid, key in [(1, "priceNew"), (2, "priceEsf")]:
        r = get("/ysfcjxx/marketInfoShow/getHousePriceIndex", {"type": tid})
        if r and r.get("status") == 1 and r.get("data"):
            D[key] = [{"year": i["year"], "month": i["month"],
                        "idx": float(i["price_total"]),
                        "chg": float(i["price_increase_rate"])}
                       for i in r["data"].get("listData", [])]
        time.sleep(0.2)

    # ========== 8. 月度成交 ==========
    print("  [8/9] 月度成交...")
    # 一手住宅上月 = 预售(ysfMsg) + 现售(xsfMsg)
    # ysfcjgs2ForMonth 的 dealCount = 上月一手住宅预售成交 (751)
    # getLastMonthXsInfoByZone 的 wqts = 上月预售成交 (547)  <- 不含现售!
    # 现售数据: 从 xsfMsg 汇总文本解析
    ym = []
    for z in ZONES:
        r = get("/ysfcjxxnew/getLastMonthXsInfoByZone", {"zone": z})
        item = get_residential(sl(r))
        ym.append({"zone": z, "ts": item.get("wqts", 0), "mj": item.get("wqmj", 0)})
        time.sleep(0.15)
    D["ysfMonth"] = ym

    # 二手住宅月度
    em = []
    for z in ZONES:
        r = get("/esfCjxxNew/esfcjgsMonth", {"zone": z})
        item = get_residential(sl(r_esf_m) if z == "全市" else sl(r))
        if z != "全市":
            item = get_residential(sl(r))

        if not item:
            item = next((i for i in sl(r) if i.get("usage") == "住宅" or i.get("reportcatalog") == "住宅"), {})
        ts = item.get("contractCount", item.get("dealCount", 0))
        em.append({"zone": z, "ts": ts})
        if z != "全市":
            time.sleep(0.15)
    D["esfMonth"] = em

    # ========== 9. 汇总消息 ==========
    print("  [9/9] 汇总...")
    msgs = {}
    for k, p in [("ysf", "/ysfcjxxnew/getYsfMsg"), ("esf", "/esfCjxxNew/getEsfMsg"), ("xsf", "/ysfcjxxnew/getXsfMsg")]:
        r = get(p)
        msgs[k] = r.get("data", "") if r and r.get("status") == 200 else ""
    D["summaries"] = msgs
    D["updateTime"] = datetime.now().strftime('%Y-%m-%d %H:%M')
    D["refMonth"] = ref_month

    # ========== 10. 成交均价数据 ==========
    print("  [附加] 均价数据...")
    avg_data_path = os.path.join(DIR, "sz_avg_price.json")
    if os.path.exists(avg_data_path):
        try:
            with open(avg_data_path, 'r', encoding='utf-8') as f:
                D["avgPrice"] = json.load(f)
            print(f"    均价数据已加载 ({len(D['avgPrice'].get('newHouse', []))}月新房)")
        except:
            D["avgPrice"] = {"newHouse": [], "secondHand": []}
    else:
        D["avgPrice"] = {
            "newHouse": [
                {"year": "2025", "month": "03", "avg_price": 51959, "ts": 4197, "source": "房天下"},
                {"year": "2025", "month": "04", "avg_price": 53842, "ts": 3702, "source": "房天下"},
                {"year": "2025", "month": "05", "avg_price": 52988, "ts": 3385, "source": "房天下"},
                {"year": "2025", "month": "06", "avg_price": 51820, "ts": 3375, "source": "房天下"},
                {"year": "2025", "month": "07", "avg_price": 53504, "ts": 2556, "source": "房天下"},
                {"year": "2025", "month": "08", "avg_price": 51755, "ts": 1908, "source": "房天下"},
                {"year": "2025", "month": "09", "avg_price": 51920, "ts": 2406, "source": "房天下"},
                {"year": "2025", "month": "10", "avg_price": 49833, "ts": 2792, "source": "房天下"},
                {"year": "2025", "month": "11", "avg_price": 54235, "ts": 2756, "source": "房天下"},
                {"year": "2025", "month": "12", "avg_price": 81410, "ts": 1992, "source": "房天下"},
                {"year": "2026", "month": "01", "avg_price": 63450, "ts": 2768, "source": "房天下"},
                {"year": "2026", "month": "02", "avg_price": 61366, "ts": 1586, "source": "房天下"},
            ],
            "secondHand": [
                {"year": "2025", "month": "02", "avg_price": 52000, "source": "乐有家"},
                {"year": "2025", "month": "05", "avg_price": 50000, "source": "乐有家"},
                {"year": "2025", "month": "08", "avg_price": 52000, "source": "乐有家"},
                {"year": "2025", "month": "11", "avg_price": 57000, "source": "贝壳"},
                {"year": "2025", "month": "12", "avg_price": 58000, "source": "贝壳"},
                {"year": "2026", "month": "01", "avg_price": 57800, "source": "贝壳"},
                {"year": "2026", "month": "02", "avg_price": 62000, "source": "乐有家"},
            ]
        }
        with open(avg_data_path, 'w', encoding='utf-8') as f:
            json.dump(D["avgPrice"], f, ensure_ascii=False, indent=2)

    return D


def update_html(data):
    """用新数据更新HTML仪表盘"""
    html_path = os.path.join(DIR, "sz_dashboard.html")
    with open(html_path, 'r', encoding='utf-8') as f:
        html = f.read()

    new_data = "const DATA = " + json.dumps(data, ensure_ascii=False, indent=2) + ";"
    pattern = r'const DATA = \{[\s\S]*?\n\};'
    html = re.sub(pattern, new_data, html)

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"仪表盘已更新: {html_path}")


if __name__ == "__main__":
    data = fetch()
    json_path = os.path.join(DIR, "sz_realestate_data.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    update_html(data)
    print(f"\n完成! JSON: {json_path}")
