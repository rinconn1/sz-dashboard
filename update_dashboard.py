#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深圳楼市数据仪表盘 - 一键更新脚本
执行数据采集并自动生成仪表盘HTML
"""

import json
import urllib.parse
import time
import subprocess
import sys
import os
from datetime import datetime

BASE_URL = "https://fdc.zjj.sz.gov.cn/szfdccommon"
DISTRICTS = ["全市", "罗湖", "福田", "南山", "盐田", "宝安", "龙岗", "龙华", "光明", "坪山", "大鹏", "深汕"]
WORKSPACE = "/Users/renzheng/WorkBuddy/20260320134026"


def api_get(path, params=None, retries=3):
    url = BASE_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    for attempt in range(retries):
        try:
            r = subprocess.run(
                ['curl', '-sk', '--max-time', '30',
                 '-H', 'User-Agent: Mozilla/5.0',
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
                print(f"  [FAIL] {path}: {e}")
                return None


def safe_list(data, key="list"):
    if not data or not isinstance(data, dict): return []
    d = data.get("data", data)
    if not d or not isinstance(d, dict): return []
    return d.get(key) or []


def collect_all():
    print(f"\n{'='*50}")
    print(f"深圳楼市数据采集 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}")

    result = {"updateTime": datetime.now().strftime('%Y-%m-%d %H:%M'), "dataDate": ""}

    # KPI
    print("[1/8] 核心指标...")
    r = api_get("/ysfcjxxnew/ysfcjgs1", {"zone": "全市"})
    rg_h = next((i for i in safe_list(r) if i.get("reportcatalog") == "住宅"), {})
    result["dataDate"] = r.get("data", {}).get("xmlDateDay", "") if r else ""
    wq_h = {}
    r2 = api_get("/ysfcjxxnew/getXsInfoByZone", {"zone": "全市"})
    wq_h = next((i for i in safe_list(r2) if i.get("useage") == "住宅"), {})

    esf_h = {}
    r3 = api_get("/esfCjxxNew/esfcjgsDay", {"zone": "全市"})
    esf_h = next((i for i in safe_list(r3) if i.get("usage") == "住宅"), {})

    inv_h = {}
    r4 = api_get("/ysfcjxxnew/ysfcjgs2ForMonth", {"zone": "全市"})
    inv_h = next((i for i in safe_list(r4) if i.get("useAge") == "住宅"), {})

    result["kpi"] = {
        "rg_ts": rg_h.get("rgts", 0), "rg_mj": rg_h.get("rgarea", 0),
        "wq_ts": wq_h.get("wqts", 0), "wq_mj": wq_h.get("wqmj", 0),
        "esf_ts": esf_h.get("contractCount", 0), "esf_mj": esf_h.get("buildingArea", 0),
        "kc_ts": inv_h.get("marketableCount", 0), "kc_mj": inv_h.get("marketableArea", 0),
        "dh_month": round(inv_h.get("marketableCount", 0) / max(inv_h.get("dealCount", 1), 1), 1),
        "last_deal": inv_h.get("dealCount", 0)
    }
    print(f"  认购{result['kpi']['rg_ts']}套 网签{result['kpi']['wq_ts']}套 二手{result['kpi']['esf_ts']}套 库存{result['kpi']['kc_ts']}套")

    # 分区认购
    print("[2/8] 分区认购...")
    rg_district = []
    for z in DISTRICTS:
        if z == "全市": continue
        r = api_get("/ysfcjxxnew/ysfcjgs1", {"zone": z})
        item = next((i for i in safe_list(r) if i.get("reportcatalog") == "住宅"), {})
        rg_district.append({"name": z, "value": item.get("rgts", 0)})
        time.sleep(0.15)
    result["rg_district"] = rg_district

    # 分区网签
    print("[3/8] 分区网签...")
    wq_data = api_get("/ysfcjxxnew/getXsCjxxGsData")
    wq_district = []
    if wq_data and wq_data.get("status") == 200 and wq_data.get("data"):
        for item in wq_data["data"].get("dataTs", []):
            wq_district.append({"name": item["name"], "value": item["value"]})
    result["wq_district"] = wq_district

    # 去化周期
    print("[4/8] 库存与去化...")
    deho = []
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/ysfcjgs2ForMonth", {"zone": z})
        inv = next((i for i in safe_list(r) if i.get("useAge") == "住宅"), {})
        deal = inv.get("dealCount", 0)
        inv_count = inv.get("marketableCount", 0)
        cycle = round(inv_count / deal, 1) if deal > 0 else -1
        deho.append({
            "zone": z, "inv": inv_count,
            "area": round(inv.get("marketableArea", 0) / 10000, 2),
            "deal": deal, "rg": 0, "cycle": cycle
        })
        time.sleep(0.15)
    result["deho"] = deho

    # 新批准预售
    print("[5/8] 新批准预售...")
    r = api_get("/ysfcjxxnew/xssList")
    xss = r.get("data", []) if r and r.get("status") == 200 else []
    result["xss"] = [{"district": x.get("district",""), "spzfts": x.get("spzfts",0), "spfts": x.get("spfts",0)} for x in xss]

    # 价格指数
    print("[6/8] 价格指数...")
    price = {}
    for tid, tname in [(1, "New"), (2, "Esf")]:
        r = api_get("/ysfcjxx/marketInfoShow/getHousePriceIndex", {"type": tid})
        if r and r.get("status") == 1 and r.get("data"):
            price[tname] = [{"year": i["year"], "month": i["month"], "idx": float(i["price_total"]), "chg": float(i["price_increase_rate"])} for i in r["data"].get("listData", [])]
        time.sleep(0.2)
    result["priceNew"] = price.get("New", [])
    result["priceEsf"] = price.get("Esf", [])

    # 月度成交
    print("[7/8] 月度成交...")
    ysf_m = []
    for z in DISTRICTS:
        r = api_get("/ysfcjxxnew/getLastMonthXsInfoByZone", {"zone": z})
        item = next((i for i in safe_list(r) if i.get("useage") == "住宅"), {})
        if item.get("wqts", 0) > 0 or z == "全市":
            ysf_m.append({"zone": z, "ts": item.get("wqts", 0)})
        time.sleep(0.15)
    result["ysfMonth"] = ysf_m

    esf_m = []
    for z in DISTRICTS:
        r = api_get("/esfCjxxNew/esfcjgsMonth", {"zone": z})
        item = next((i for i in safe_list(r) if i.get("usage") == "住宅"), {})
        if not item:
            item = next((i for i in safe_list(r) if i.get("reportcatalog") == "住宅"), {})
        ts = item.get("contractCount", item.get("dealCount", 0))
        if ts > 0 or z == "全市":
            esf_m.append({"zone": z, "ts": ts})
        time.sleep(0.15)
    result["esfMonth"] = esf_m

    # 汇总消息
    print("[8/8] 市场汇总...")
    msgs = {}
    for key, path in [("ysf", "/ysfcjxxnew/getYsfMsg"), ("esf", "/esfCjxxNew/getEsfMsg"), ("xsf", "/ysfcjxxnew/getXsfMsg")]:
        r = api_get(path)
        if r and r.get("status") == 200 and r.get("data"):
            msgs[key] = r["data"]
        else:
            msgs[key] = ""
    result["summaries"] = msgs

    # 保存JSON
    json_path = os.path.join(WORKSPACE, "sz_realestate_data.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n数据保存至: {json_path}")
    return result


def generate_dashboard(data):
    """将数据嵌入HTML模板并生成仪表盘"""
    print("生成仪表盘...")

    def j(obj): return json.dumps(obj, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>深圳楼市数据仪表盘</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
    <style>
        :root{{
            --bg:#0f1923;--card:#1a2733;--card2:#1f2f3d;--tx:#e8edf2;--tx2:#8899aa;--tx3:#556677;
            --blue:#4facfe;--green:#43e97b;--orange:#f6d365;--red:#f5576c;--purple:#a18cd1;--cyan:#38f9d7;
            --gap:14px;--r:12px;
        }}
        *{{margin:0;padding:0;box-sizing:border-box}}
        body{{font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display','PingFang SC','Microsoft YaHei',sans-serif;background:var(--bg);color:var(--tx);line-height:1.5}}
        .dash{{max-width:1440px;margin:0 auto;padding:var(--gap)}}
        .hdr{{background:linear-gradient(135deg,#0d1520,#1a2733);border:1px solid rgba(79,172,254,.12);border-radius:var(--r);padding:18px 24px;margin-bottom:var(--gap);display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:14px}}
        .hdr h1{{font-size:20px;font-weight:700;background:linear-gradient(90deg,var(--blue),var(--cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
        .hdr .sub{{font-size:11px;color:var(--tx2);margin-top:2px}}
        .hdr .src{{font-size:11px;color:var(--tx3);background:rgba(255,255,255,.04);padding:5px 12px;border-radius:6px}}
        .stl{{font-size:14px;font-weight:600;margin:20px 0 10px;padding-left:10px;border-left:3px solid var(--blue)}}
        .kpir{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:var(--gap);margin-bottom:var(--gap)}}
        .kpc{{background:var(--card);border-radius:var(--r);padding:16px 18px;border:1px solid rgba(255,255,255,.03);position:relative;overflow:hidden;transition:all .2s}}
        .kpc::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px}}
        .kpc.c1::before{{background:linear-gradient(90deg,var(--blue),#00f2fe)}}
        .kpc.c2::before{{background:linear-gradient(90deg,var(--green),#38f9d7)}}
        .kpc.c3::before{{background:linear-gradient(90deg,var(--orange),#fda085)}}
        .kpc.c4::before{{background:linear-gradient(90deg,var(--red),#ff9a9e)}}
        .kpc.c5::before{{background:linear-gradient(90deg,var(--purple),#fbc2eb)}}
        .kpc:hover{{background:var(--card2);transform:translateY(-2px);box-shadow:0 4px 20px rgba(0,0,0,.3)}}
        .kl{{font-size:11px;color:var(--tx2);margin-bottom:6px;letter-spacing:.5px}}
        .kv{{font-size:28px;font-weight:800;margin-bottom:4px}}
        .kv.v1{{color:var(--blue)}}.kv.v2{{color:var(--green)}}.kv.v3{{color:var(--orange)}}.kv.v4{{color:var(--red)}}.kv.v5{{color:var(--purple)}}
        .ku{{font-size:13px;font-weight:400;color:var(--tx2);margin-left:3px}}
        .ks{{font-size:11px;color:var(--tx3)}}
        .cr{{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:var(--gap);margin-bottom:var(--gap)}}
        .cr.f1{{grid-template-columns:1fr}}
        .cc{{background:var(--card);border-radius:var(--r);padding:18px 22px;border:1px solid rgba(255,255,255,.03)}}
        .ch{{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}}
        .ct{{font-size:13px;font-weight:600}}
        .cb{{font-size:10px;color:var(--tx3);background:rgba(255,255,255,.04);padding:2px 10px;border-radius:10px}}
        .cw{{position:relative;height:300px}}
        .cw canvas{{width:100%!important;height:100%!important}}
        .sr{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:var(--gap);margin-bottom:var(--gap)}}
        .sc{{background:var(--card);border-radius:var(--r);padding:16px 20px;border:1px solid rgba(255,255,255,.03)}}
        .sc h4{{font-size:12px;font-weight:600;margin-bottom:6px}}
        .sc p{{font-size:11px;color:var(--tx2);line-height:1.8}}
        .sc p b{{color:var(--tx)}}
        .ts{{background:var(--card);border-radius:var(--r);padding:18px 22px;border:1px solid rgba(255,255,255,.03);overflow-x:auto}}
        table{{width:100%;border-collapse:collapse;font-size:12px}}
        th{{text-align:left;padding:9px 10px;border-bottom:1px solid rgba(255,255,255,.06);color:var(--tx2);font-weight:600;font-size:11px;white-space:nowrap;user-select:none;cursor:pointer}}
        th:hover{{color:var(--blue)}}
        td{{padding:9px 10px;border-bottom:1px solid rgba(255,255,255,.02)}}
        tr:hover{{background:rgba(79,172,254,.04)}}
        .n{{text-align:right;font-variant-numeric:tabular-nums}}
        .bar{{height:7px;border-radius:4px;background:rgba(255,255,255,.05);min-width:70px;display:inline-block;vertical-align:middle}}
        .bf{{height:100%;border-radius:4px}}
        .tg{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600}}
        .td{{background:rgba(245,87,108,.12);color:var(--red)}}
        .tw{{background:rgba(246,211,101,.12);color:var(--orange)}}
        .to{{background:rgba(67,233,123,.12);color:var(--green)}}
        .ft{{text-align:center;padding:18px;font-size:11px;color:var(--tx3)}}
        @media(max-width:768px){{.kpir{{grid-template-columns:repeat(2,1fr)}}.cr{{grid-template-columns:1fr}}}}
    </style>
</head>
<body>
<div class="dash">
    <header class="hdr">
        <div><h1><i class="fa-solid fa-city" style="margin-right:8px"></i>深圳楼市实时数据仪表盘</h1>
        <div class="sub">数据来源：深圳市房地产信息平台 | 更新：<span id="ut">{data["updateTime"]}</span></div></div>
        <span class="src"><i class="fa-solid fa-database" style="margin-right:4px"></i>一手房预售 + 二手房成交</span>
    </header>

    <div class="stl">核心指标 (截至 {data["dataDate"]})</div>
    <div class="kpir">
        <div class="kpc c1"><div class="kl"><i class="fa-solid fa-file-signature"></i> 今日住宅认购</div><div class="kv v1">{data["kpi"]["rg_ts"]}<span class="ku">套</span></div><div class="ks">面积 {(data["kpi"]["rg_mj"]/10000):.2f} 万m²</div></div>
        <div class="kpc c2"><div class="kl"><i class="fa-solid fa-file-contract"></i> 今日住宅网签</div><div class="kv v2">{data["kpi"]["wq_ts"]}<span class="ku">套</span></div><div class="ks">面积 {(data["kpi"]["wq_mj"]/10000):.2f} 万m²</div></div>
        <div class="kpc c3"><div class="kl"><i class="fa-solid fa-house-chimney"></i> 今日二手住宅成交</div><div class="kv v3">{data["kpi"]["esf_ts"]}<span class="ku">套</span></div><div class="ks">面积 {(data["kpi"]["esf_mj"]/10000):.2f} 万m²</div></div>
        <div class="kpc c4"><div class="kl"><i class="fa-solid fa-warehouse"></i> 住宅库存(期房待售)</div><div class="kv v4">{data["kpi"]["kc_ts"]}<span class="ku">套</span></div><div class="ks">面积 {(data["kpi"]["kc_mj"]/10000):.2f} 万m²</div></div>
        <div class="kpc c5"><div class="kl"><i class="fa-solid fa-clock"></i> 去化周期</div><div class="kv v5">{data["kpi"]["dh_month"]}<span class="ku">月</span></div><div class="ks">上月成交 {data["kpi"]["last_deal"]} 套</div></div>
    </div>

    <div class="stl">市场概况 (上月)</div>
    <div class="sr">
        <div class="sc"><h4><i class="fa-solid fa-building" style="color:var(--blue)"></i> 一手房预售成交</h4><p>{data["summaries"]["ysf"]}</p></div>
        <div class="sc"><h4><i class="fa-solid fa-house-circle-check" style="color:var(--orange)"></i> 二手房成交</h4><p>{data["summaries"]["esf"]}</p></div>
        <div class="sc"><h4><i class="fa-solid fa-key" style="color:var(--green)"></i> 一手房现售成交</h4><p>{data["summaries"]["xsf"]}</p></div>
    </div>

    <div class="stl">分区成交</div>
    <div class="cr">
        <div class="cc"><div class="ch"><span class="ct">一手房 分区认购 vs 网签 (住宅, 当日)</span><span class="cb">{data["dataDate"]}</span></div><div class="cw"><canvas id="c1"></canvas></div></div>
        <div class="cc"><div class="ch"><span class="ct">二手房 分区成交 (住宅, 上月)</span><span class="cb">2026年2月</span></div><div class="cw"><canvas id="c2"></canvas></div></div>
    </div>

    <div class="stl">库存与去化</div>
    <div class="cr">
        <div class="cc"><div class="ch"><span class="ct">各区住宅库存套数</span><span class="cb">期房待售</span></div><div class="cw"><canvas id="c3"></canvas></div></div>
        <div class="cc"><div class="ch"><span class="ct">各区去化周期 (月)</span><span class="cb">库存/上月成交</span></div><div class="cw"><canvas id="c4"></canvas></div></div>
    </div>

    <div class="stl">一手 vs 二手 月度对比</div>
    <div class="cr f1"><div class="cc"><div class="ch"><span class="ct">住宅月度成交套数对比 (上月, 按区)</span></div><div class="cw" style="height:280px"><canvas id="c5"></canvas></div></div></div>

    <div class="stl">价格走势</div>
    <div class="cr">
        <div class="cc"><div class="ch"><span class="ct">新房价格指数 (环比)</span><span class="cb">100=持平</span></div><div class="cw"><canvas id="c6"></canvas></div></div>
        <div class="cc"><div class="ch"><span class="ct">二手房价格指数 (环比)</span><span class="cb">100=持平</span></div><div class="cw"><canvas id="c7"></canvas></div></div>
    </div>

    <div class="stl">各区库存去化明细</div>
    <div class="ts"><table><thead><tr><th>区域</th><th class="n">库存套数</th><th class="n">库存面积(万m²)</th><th class="n">上月成交</th><th class="n">去化周期</th><th>压力</th><th>占比</th></tr></thead><tbody id="tb"></tbody></table></div>

    <div class="ft">数据来源：深圳市住房和建设局 · 深圳市房地产信息平台 (fdc.zjj.sz.gov.cn) | 仅供参考</div>
</div>
<script>
const D={j()}}};
// rest of JS will be in the template
</script>
</body>
</html>"""

    # 这种方式太复杂，让我改用直接替换数据的方式
    # 读取现有HTML，替换DATA对象
    template_path = os.path.join(WORKSPACE, "sz_dashboard_template.html")
    output_path = os.path.join(WORKSPACE, "sz_dashboard.html")

    # 读取模板（如果不存在则用当前仪表盘）
    if not os.path.exists(template_path):
        print("  无模板文件，使用当前仪表盘")
        return False

    with open(template_path, 'r', encoding='utf-8') as f:
        html = f.read()

    # 替换数据
    html = html.replace('const DATA = {', f'const DATA = {j(data)} // AUTO-GENERATED')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"仪表盘已生成: {output_path}")
    return True


if __name__ == "__main__":
    data = collect_all()
    # 保存JSON
    json_path = os.path.join(WORKSPACE, "sz_realestate_data.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n完成! JSON: {json_path}")
