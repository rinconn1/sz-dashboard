#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深圳市房地产数据采集脚本 v2
"""

import json
import urllib.parse
import time
import subprocess
from datetime import datetime
from typing import Optional

BASE_URL = "https://fdc.zjj.sz.gov.cn/szfdccommon"
DISTRICTS = ["全市", "罗湖", "福田", "南山", "盐田", "宝安", "龙岗", "龙华", "光明", "坪山", "大鹏", "深汕"]


def api_get(path, params=None, retries=3):
    url = BASE_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for attempt in range(retries):
        try:
            r = subprocess.run(
                ['curl', '-sk', '--max-time', '30',
                 '-H', 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
                 '-H', 'Accept: application/json',
                 '-H', 'Referer: https://fdc.zjj.sz.gov.cn/',
                 url],
                capture_output=True, text=True, timeout=35
            )
            if r.returncode == 0 and r.stdout.strip().startswith('{'):
                return json.loads(r.stdout)
            raise Exception(f"HTTP {r.returncode}")
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(0.5)
            else:
                print(f"  [FAIL] {path} zone={params.get('zone','') if params else ''}: {e}")
                return None


def safe_list(data, key="list"):
    """安全获取列表数据"""
    if not data or not isinstance(data, dict):
        return []
    d = data.get("data", data)
    if not d or not isinstance(d, dict):
        return []
    return d.get(key) or []


def main():
    print(f"\n{'='*60}")
    print(f"深圳市房地产数据采集")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    all_data = {"采集时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    # 1. 一手房认购数据 (ysfcjgs1) - 认购书数据
    print("[1/9] 一手房认购数据...")
    rg_data = {}
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/ysfcjgs1", {"zone": z})
        for item in safe_list(r):
            rg_data[f"{z}_{item.get('reportcatalog','')}"] = {
                "认购套数": item.get("rgts", 0),
                "认购面积": item.get("rgarea", 0),
                "日期": r.get("data", {}).get("xmlDateDay", "") if r else ""
            }
        time.sleep(0.2)
    print(f"  OK: {len(rg_data)} 条")
    all_data["认购"] = rg_data

    # 2. 一手房网签数据 (getXsInfoByZone) - 已录入合同
    print("[2/9] 一手房网签数据...")
    wq_data = {}
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/getXsInfoByZone", {"zone": z})
        for item in safe_list(r):
            wq_data[f"{z}_{item.get('useage','')}"] = {
                "网签套数": item.get("wqts", 0),
                "网签面积": item.get("wqmj", 0),
                "日期": r.get("data", {}).get("xmlDateDay", "") if r else ""
            }
        time.sleep(0.2)
    print(f"  OK: {len(wq_data)} 条")
    all_data["网签"] = wq_data

    # 3. 上月网签数据
    print("[3/9] 上月网签数据...")
    lm_data = {}
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/getLastMonthXsInfoByZone", {"zone": z})
        for item in safe_list(r):
            lm_data[f"{z}_{item.get('useage','')}"] = {
                "网签套数": item.get("wqts", 0),
                "网签面积": item.get("wqmj", 0),
                "日期": r.get("data", {}).get("xmlDateMonth", r.get("data", {}).get("xmlDateDay", "")) if r else ""
            }
        time.sleep(0.2)
    print(f"  OK: {len(lm_data)} 条")
    all_data["上月网签"] = lm_data

    # 4. 库存数据 (ysfcjgs2ForMonth) - 期房待售
    print("[4/9] 库存数据...")
    inv_data = {}
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/ysfcjgs2ForMonth", {"zone": z})
        for item in safe_list(r):
            inv_data[f"{z}_{item.get('useAge','')}"] = {
                "待售套数": item.get("marketableCount", 0),
                "待售面积": item.get("marketableArea", 0),
                "成交套数": item.get("dealCount", 0),
                "成交面积": item.get("dealArea", 0),
                "日期": r.get("data", {}).get("xmlDateMonth", "") if r else ""
            }
        time.sleep(0.2)
    print(f"  OK: {len(inv_data)} 条")
    all_data["库存"] = inv_data

    # 5. 月度认购 (ysfcjgs1ForMonth)
    print("[5/9] 月度认购数据...")
    rgm_data = {}
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/ysfcjgs1ForMonth", {"zone": z})
        for item in safe_list(r):
            rgm_data[f"{z}_{item.get('reportcatalog','')}"] = {
                "认购套数": item.get("rgts", 0),
                "认购面积": item.get("rgarea", 0),
                "日期": r.get("data", {}).get("xmlDateMonth", r.get("data", {}).get("xmlDateDay", "")) if r else ""
            }
        time.sleep(0.2)
    print(f"  OK: {len(rgm_data)} 条")
    all_data["月度认购"] = rgm_data

    # 6. 新批准预售 (xssList)
    print("[6/9] 新批准预售数据...")
    r = api_get("/ysfcjxxnew/xssList")
    xss = []
    if r and r.get("status") == 200 and isinstance(r.get("data"), list):
        xss = r["data"]
    print(f"  OK: {len(xss)} 条")
    all_data["新批准预售"] = xss

    # 7. 价格指数
    print("[7/9] 价格指数...")
    price = {}
    for tid, tname in [(1, "新房"), (2, "二手房")]:
        r = api_get("/ysfcjxx/marketInfoShow/getHousePriceIndex", {"type": tid})
        if r and r.get("status") == 1 and r.get("data"):
            price[tname] = r["data"].get("listData", [])
        time.sleep(0.2)
    print(f"  OK: 新房 {len(price.get('新房',[]))} 月, 二手房 {len(price.get('二手房',[]))} 月")
    all_data["价格指数"] = price

    # 8. 二手房成交
    print("[8/9] 二手房成交数据...")
    esf_day = {}
    for z in DISTRICTS:
        r = api_get("/esfCjxxNew/esfcjgsDay", {"zone": z})
        for item in safe_list(r):
            esf_day[f"{z}_{item.get('usage','')}"] = {
                "成交套数": item.get("contractCount", 0),
                "成交面积": item.get("buildingArea", 0),
                "日期": r.get("data", {}).get("xmlDateDay", "") if r else ""
            }
        time.sleep(0.2)
    
    esf_month = {}
    for z in DISTRICTS:
        r = api_get("/esfCjxxNew/esfcjgsMonth", {"zone": z})
        for item in safe_list(r):
            usage = item.get("usage", item.get("reportcatalog", ""))
            esf_month[f"{z}_{usage}"] = {
                "成交套数": item.get("contractCount", item.get("dealCount", 0)),
                "成交面积": item.get("buildingArea", item.get("dealArea", 0)),
                "日期": r.get("data", {}).get("xmlDateMonth", r.get("data", {}).get("xmlDateDay", "")) if r else ""
            }
        time.sleep(0.2)
    
    # 汇总消息
    esf_msg = ""
    r = api_get("/esfCjxxNew/getEsfMsg")
    if r and r.get("status") == 200 and r.get("data"):
        esf_msg = r["data"]
    
    print(f"  OK: 日 {len(esf_day)} 条, 月 {len(esf_month)} 条")
    all_data["二手房"] = {"日成交": esf_day, "月成交": esf_month, "汇总": esf_msg}

    # 9. 分区网签趋势 (getXsCjxxGsData)
    print("[9/9] 分区网签趋势...")
    r = api_get("/ysfcjxxnew/getXsCjxxGsData")
    trend = {}
    if r and r.get("status") == 200 and r.get("data"):
        trend = {
            "日期": r["data"].get("xmlDateDay", ""),
            "分区套数": r["data"].get("dataTs", []),
            "分区面积": r["data"].get("dataMj", [])
        }
    print(f"  OK: {len(trend.get('分区套数', []))} 区")
    all_data["分区趋势"] = trend

    # 10. 计算去化周期
    print("\n计算去化周期...")
    zones = [z for z in DISTRICTS if z != "全市"]
    deho = []
    for z in ["全市"] + zones:
        ik = f"{z}_住宅"
        inv = inv_data.get(ik, {})
        rgm = rgm_data.get(ik, {})
        inv_count = inv.get("待售套数", 0)
        deal_count = inv.get("成交套数", 0)
        rg_count = rgm.get("认购套数", 0)
        monthly_avg = deal_count if deal_count > 0 else rg_count
        cycle = round(inv_count / monthly_avg, 1) if monthly_avg > 0 else -1
        deho.append({
            "区域": z,
            "库存套数": inv_count,
            "库存面积万平": round(inv.get("待售面积", 0) / 10000, 2),
            "上月成交套数": deal_count,
            "去化周期月": cycle
        })
        if cycle >= 0:
            print(f"  {z}: 库存{inv_count}套, 月均{monthly_avg}套, 去化{cycle}月")
        else:
            print(f"  {z}: 库存{inv_count}套, 暂无成交数据")
    all_data["去化周期"] = deho

    # 保存
    out = "/Users/renzheng/WorkBuddy/20260320134026/sz_realestate_data.json"
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"\n{'='*60}")
    print(f"完成! 数据保存至: {out}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
