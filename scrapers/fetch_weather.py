import requests
import json
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()

# ── CONFIGURATION ─────────────────────────────────────────────────────────
KSDMA_URL = "https://sdma.kerala.gov.in/temperature/"
BASE_URL = "https://sdma.kerala.gov.in"
URLS = {
    "forecast": "https://dss.imd.gov.in/dwr_img/GIS/CD_Status_Forecast.json",
    "observed": "https://dss.imd.gov.in/dwr_img/GIS/Observed_Stations.json",
    "warnings": "https://dss.imd.gov.in/dwr_img/GIS/HWCW_Status.json",
}

# Precise Mapping for Clean Names (Coordinates -> Precise Name)
# This prevents "AWS 11.03" by mapping coordinates to your preferred names.
PRECISE_NAMES = [
    {"name": "Thiruvananthapuram (Airport)", "lat": 8.48, "lon": 76.95},
    {"name": "Punalur", "lat": 9.01, "lon": 76.92},
    {"name": "Alappuzha", "lat": 9.49, "lon": 76.33},
    {"name": "Kottayam", "lat": 9.53, "lon": 76.60},
    {"name": "Kochi", "lat": 10.23, "lon": 76.40},
    {"name": "Vellanikara", "lat": 10.54, "lon": 76.27},
    {"name": "Palakkad", "lat": 10.77, "lon": 76.65},
    {"name": "Kozhikode", "lat": 11.25, "lon": 75.77},
    {"name": "Kannur", "lat": 11.91, "lon": 75.36},
    {"name": "Tirur", "lat": 10.91, "lon": 75.92},
    {"name": "Thavanur", "lat": 10.84, "lon": 76.00}
]

# ── SCRAPE KSDMA DATA (Text & Maps) ───────────────────────────────────────
def get_ksdma_meta():
    try:
        r = requests.get(KSDMA_URL, timeout=20, verify=False)
        soup = BeautifulSoup(r.text, 'html.parser')
        maps = {"max": None, "min": None, "humid": None}
        
        # Link Chasing for Maps
        max_l = soup.find('a', string=re.compile("Maximum Temperature", re.I))
        if max_l: maps["max"] = max_l.get('href') if max_l.get('href').startswith('http') else BASE_URL + max_l.get('href')
        
        humid_img = soup.find('img', src=re.compile("Hot-Humid", re.I))
        if humid_img: maps["humid"] = humid_img.get('src') if humid_img.get('src').startswith('http') else BASE_URL + humid_img.get('src')

        all_p = [p.get_text(strip=True) for p in soup.find_all('p') if len(p.get_text()) > 30]
        return {
            "alert_paragraphs": all_p[:3],
            "max_map": maps["max"],
            "humid_map": maps["humid"],
            "sync_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
    except: return {"alert_paragraphs": ["Data temporarily unavailable"], "sync_at": "Error"}

# ── PROCESS STATION NAMES (The Fuzzy Fix) ─────────────────────────────────
def get_precise_name(lat, lon, original_name):
    for target in PRECISE_NAMES:
        if abs(lat - target['lat']) < 0.1 and abs(lon - target['lon']) < 0.1:
            return target['name']
    return original_name if original_name and not original_name.isdigit() else f"Station {lat}"

# ── MAIN FETCH FUNCTION ───────────────────────────────────────────────────
def fetch_all():
    print("🚀 Running Unified War Room Sync...")
    
    # 1. Get KSDMA Official Alerts/Maps
    meta = get_ksdma_meta()

    # 2. Fetch IMD JSONs
    forecast_res = requests.get(URLS["forecast"], verify=False).json()
    stations = []

    for feature in forecast_res.get("features", []):
        p = feature["properties"]
        lat, lon = p.get("Latitude"), p.get("Longitude")
        
        # Filter for Kerala Boundary
        if lat and lon and 8.0 <= lat <= 13.0 and 74.5 <= lon <= 77.5:
            temp = p.get("D1F_Mx_Tem") or 0
            
            # Determine Status
            status = "Normal"
            if temp >= 38: status = "Alert"
            elif temp >= 36: status = "Watch"

            stations.append({
                "name": get_precise_name(lat, lon, p.get("Stat_Name")),
                "lat": lat, "lon": lon,
                "temp": temp,
                "status": status,
                "departure": p.get("D1F_Mx_Dep", 0),
                "humidity": p.get("D1_RH_0830", 0),
                "rainfall": p.get("D1_Rainfall", 0),
                "warning_code": p.get("warning_color", 4)
            })

    # 3. Create Final Unified JSON
    output = {
        "meta": meta,
        "stations": sorted(stations, key=lambda x: x['lat'])
    }

    os.makedirs("data", exist_ok=True)
    with open("data/weather.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved {len(stations)} stations with KSDMA Alerts.")

if __name__ == "__main__":
    fetch_all()
