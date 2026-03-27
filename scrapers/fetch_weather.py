import requests
import json
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()  # IMD uses non-standard SSL cert

# ── OFFICIAL KERALA STATIONS (IMD Station Directory — ground truth) ────────
KERALA_OFFICIAL = {
    "43352": {"name": "Alappuzha",                    "district": "Alappuzha"},
    "43315": {"name": "Kannur",                       "district": "Kannur"},
    "43253": {"name": "Kannur AMS",                   "district": "Kannur"},
    "43336": {"name": "Kochi (CIAL)",                 "district": "Ernakulam"},
    "43353": {"name": "Kochi (NAS)",                  "district": "Ernakulam"},
    "43355": {"name": "Kottayam",                     "district": "Kottayam"},
    "43314": {"name": "Kozhikode",                    "district": "Kozhikode"},
    "43320": {"name": "Kozhikode (Airport)",          "district": "Malappuram"},
    "43335": {"name": "Palakkad",                     "district": "Palakkad"},
    "43354": {"name": "Punalur",                      "district": "Kollam"},
    "43371": {"name": "Thiruvananthapuram",           "district": "Thiruvananthapuram"},
    "43372": {"name": "Thiruvananthapuram (Airport)", "district": "Thiruvananthapuram"},
    "43357": {"name": "Thrissur (Vellanikara)",       "district": "Thrissur"},
}

# ── KERALA BOUNDS FOR OBSERVED AWS STATIONS ───────────────────────────────
KL_LAT_MIN, KL_LAT_MAX = 8.0,  12.5
KL_LON_MIN, KL_LON_MAX = 74.8, 76.8

EXCLUDE_NAMES = [
    "OOTY", "MANDYA", "CHANDUR", "COIMBATORE",
    "MYSORE", "MYSURU", "UDHAGAMANDALAM", "COONOOR",
    "BANGALORE", "BENGALURU"
]

# ── URLs ──────────────────────────────────────────────────────────────────
IMD_URLS = {
    "forecast": "https://dss.imd.gov.in/dwr_img/GIS/CD_Status_Forecast.json",
    "observed": "https://dss.imd.gov.in/dwr_img/GIS/Observed_Stations.json",
    "warnings": "https://dss.imd.gov.in/dwr_img/GIS/HWCW_Status.json",
}
KSDMA_URL  = "https://sdma.kerala.gov.in/temperature/"
KSDMA_BASE = "https://sdma.kerala.gov.in"

IMD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer":    "https://dss.imd.gov.in/dwr_img/GIS/heatwave.html",
}

# ── FETCH JSON ────────────────────────────────────────────────────────────
def fetch_json(url, label):
    try:
        r = requests.get(url, headers=IMD_HEADERS, timeout=30, verify=False)
        if r.status_code == 200:
            print(f"  ✅ {label} fetched")
            return r.json()
        print(f"  ❌ {label} — HTTP {r.status_code}")
        return None
    except Exception as e:
        print(f"  ❌ {label} — {e}")
        return None

# ── SCRAPE KSDMA ──────────────────────────────────────────────────────────
def get_ksdma_meta():
    print("  Scraping KSDMA...")
    try:
        r = requests.get(KSDMA_URL, timeout=20, verify=False,
                         headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")

        # Alert paragraphs — any <p> longer than 30 chars
        paragraphs = [
            p.get_text(strip=True)
            for p in soup.find_all("p")
            if len(p.get_text(strip=True)) > 30
        ]

        # Maximum temperature map
        max_map = None
        max_link = soup.find("a", string=re.compile("maximum temperature", re.I))
        if max_link and max_link.get("href"):
            href = max_link["href"]
            max_map = href if href.startswith("http") else KSDMA_BASE + href

        # Minimum temperature map
        min_map = None
        min_link = soup.find("a", string=re.compile("minimum temperature", re.I))
        if min_link and min_link.get("href"):
            href = min_link["href"]
            min_map = href if href.startswith("http") else KSDMA_BASE + href

        # Humidity map
        humid_map = None
        humid_img = soup.find("img", src=re.compile("humid|hot", re.I))
        if humid_img and humid_img.get("src"):
            src = humid_img["src"]
            humid_map = src if src.startswith("http") else KSDMA_BASE + src

        print(f"  ✅ KSDMA — {len(paragraphs)} paragraphs, max={bool(max_map)}, min={bool(min_map)}, humid={bool(humid_map)}")
        return {
            "ksdma_source":     "Kerala State Disaster Management Authority",
            "alert_paragraphs": paragraphs[:4],
            "max_map":          max_map,
            "min_map":          min_map,
            "humid_map":        humid_map,
        }

    except Exception as e:
        print(f"  ❌ KSDMA failed — {e}")
        return {
            "ksdma_source":     "Kerala State Disaster Management Authority",
            "alert_paragraphs": ["Alert data temporarily unavailable. Please check sdma.kerala.gov.in"],
            "max_map":          None,
            "min_map":          None,
            "humid_map":        None,
        }

# ── CHECK KERALA BOUNDARY ─────────────────────────────────────────────────
def is_kerala_aws(lat, lon):
    if lat is None or lon is None:
        return False
    try:
        return (KL_LAT_MIN <= float(lat) <= KL_LAT_MAX and
                KL_LON_MIN <= float(lon) <= KL_LON_MAX)
    except:
        return False

# ── EXTRACT STATION ───────────────────────────────────────────────────────
def extract(props, name, district, source):
    return {
        "stat_code":  str(props.get("Stat_Code", "")),
        "name":       name,
        "district":   district,
        "lat":        props.get("Latitude"),
        "lon":        props.get("Longitude"),
        "source":     source,

        # Yesterday actual
        "obs_max":    props.get("PD_Mx_Temp"),
        "obs_dep":    props.get("PD_Mx_Dep"),
        "rainfall":   props.get("Pt_24_Rain"),
        "humidity":   props.get("D1_RH_0830"),

        # Today forecast
        "fc_max":     props.get("D1F_Mx_Tem"),
        "fc_min":     props.get("D1F_Mn_Tem"),
        "fc_dep":     props.get("D1F_Mx_Dep"),
        "fc_weather": props.get("D1F_Weathr"),

        # 7-day forecast
        "day2_max": props.get("D2_Mx_Temp"), "day2_min": props.get("D2_Mn_Temp"), "day2_weather": props.get("D2_Weather"),
        "day3_max": props.get("D3_Mx_Temp"), "day3_min": props.get("D3_Mn_Temp"), "day3_weather": props.get("D3_Weather"),
        "day4_max": props.get("D4_Mx_Temp"), "day4_min": props.get("D4_Mn_Temp"), "day4_weather": props.get("D4_Weather"),
        "day5_max": props.get("D5_Mx_Temp"), "day5_min": props.get("D5_Mn_Temp"), "day5_weather": props.get("D5_Weather"),
        "day6_max": props.get("D6_Mx_Temp"), "day6_min": props.get("D6_Mn_Temp"), "day6_weather": props.get("D6_Weather"),
        "day7_max": props.get("D7_Mx_Temp"), "day7_min": props.get("D7_Mn_Temp"), "day7_weather": props.get("D7_Weather"),

        "sunrise":    props.get("Sr_Time"),
        "sunset":     props.get("Ss_Time"),

        # Filled later
        "warning_color": None,
        "alert":         None,
    }

# ── ALERT LEVEL ───────────────────────────────────────────────────────────
def get_alert(temp):
    if temp is None:
        return {"level": "unknown", "ml": "വിവരമില്ല", "en": "No Data"}
    t = float(temp)
    if t >= 42: return {"level": "extreme", "ml": "അതീവ ജാഗ്രത", "en": "Extreme Danger"}
    if t >= 40: return {"level": "warning", "ml": "മുന്നറിയിപ്പ്", "en": "Warning"}
    if t >= 38: return {"level": "alert",   "ml": "അലേർട്ട്",      "en": "Alert"}
    if t >= 36: return {"level": "watch",   "ml": "ജാഗ്രത",        "en": "Watch"}
    return             {"level": "normal",  "ml": "സാധാരണ",       "en": "Normal"}

# ── MAIN ──────────────────────────────────────────────────────────────────
def fetch_all():
    print("\n🌡  Kerala Heat Watch — Data Fetch")
    print(f"    {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("─" * 52)

    forecast_data = fetch_json(IMD_URLS["forecast"], "CD_Status_Forecast")
    observed_data = fetch_json(IMD_URLS["observed"], "Observed_Stations")
    warnings_data = fetch_json(IMD_URLS["warnings"], "HWCW_Status")
    ksdma_meta    = get_ksdma_meta()

    stations = {}

    # Step 1 — Official Kerala stations from forecast file
    if forecast_data:
        for feature in forecast_data.get("features", []):
            props = feature.get("properties", {})
            code  = str(props.get("Stat_Code", ""))
            if code in KERALA_OFFICIAL:
                info = KERALA_OFFICIAL[code]
                stations[code] = extract(props, info["name"], info["district"], "forecast")

    print(f"\n  📍 Official stations: {len(stations)}/13")

    # Step 2 — Observed AWS inside Kerala boundary
    obs_count = 0
    if observed_data:
        for feature in observed_data.get("features", []):
            props     = feature.get("properties", {})
            code      = str(props.get("Stat_Code", ""))
            lat       = props.get("Latitude")
            lon       = props.get("Longitude")
            stat_name = str(props.get("Stat_Name", "")).upper()
            if code in stations: continue
            if not is_kerala_aws(lat, lon): continue
            if any(x in stat_name for x in EXCLUDE_NAMES): continue
            name = str(props.get("Stat_Name", f"AWS {code}")).title()
            stations[code] = extract(props, name, "Kerala", "observed")
            obs_count += 1

    print(f"  📡 AWS stations: {obs_count}")

    # Step 3 — Warning colours
    matched = 0
    if warnings_data:
        for feature in warnings_data.get("features", []):
            props = feature.get("properties", {})
            code  = str(props.get("Stat_Code", ""))
            if code in stations:
                color = (props.get("Day1_Color") or props.get("HW_Color") or props.get("Color"))
                stations[code]["warning_color"] = color
                matched += 1

    print(f"  🚨 Warning colours: {matched}")

    # Step 4 — Alert levels
    for s in stations.values():
        temp = s.get("fc_max") or s.get("obs_max")
        s["alert"] = get_alert(temp)

    # Step 5 — Sort south to north
    sorted_stations = sorted(
        stations.values(),
        key=lambda s: float(s.get("lat") or 0)
    )

    # Step 6 — Save atomically
    output = {
        "meta": {
            "fetched_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "station_count": len(sorted_stations),
            "imd_source":    "India Meteorological Department",
            **ksdma_meta,
        },
        "stations": sorted_stations,
    }

    os.makedirs("data", exist_ok=True)
    tmp   = "data/weather.tmp.json"
    final = "data/weather.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    os.replace(tmp, final)

    print(f"\n  💾 Saved → {final}")
    print(f"  ✅ Total: {len(sorted_stations)} stations")
    print("─" * 52)
    print(f"\n  {'Station':<34} {'Temp':>6}  Alert")
    print(f"  {'─'*34} {'─'*6}  {'─'*14}")
    for s in sorted_stations:
        temp = s.get("fc_max") or s.get("obs_max")
        tstr = f"{temp}°C" if temp else "null"
        print(f"  {s['name']:<34} {tstr:>6}  {s['alert']['en']}")

if __name__ == "__main__":
    fetch_all()
