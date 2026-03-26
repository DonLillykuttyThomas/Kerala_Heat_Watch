import requests
from bs4 import BeautifulSoup
import json
import re
import urllib3
from datetime import datetime
import os

# 1. SETUP & SECURITY
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
KSDMA_URL = "https://sdma.kerala.gov.in/temperature/"
IMD_JSON_URL = "https://dss.imd.gov.in/dwr_img/GIS/CD_Status_Forecast.json"
BASE_URL = "https://sdma.kerala.gov.in"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# 2. PRECISE TARGETS (Fuzzy Matching Database)
# If a station is within ~11km of these coords, it gets this name.
TARGETS = [
    {"name": "Thiruvananthapuram (Airport)", "lat": 8.48, "lon": 76.95},
    {"name": "Thiruvananthapuram", "lat": 8.46, "lon": 76.95},
    {"name": "Varkala", "lat": 8.73, "lon": 76.71},
    {"name": "Kollam", "lat": 8.89, "lon": 76.61},
    {"name": "Punalur", "lat": 9.01, "lon": 76.92},
    {"name": "Kottayam", "lat": 9.53, "lon": 76.60},
    {"name": "Kochi", "lat": 10.23, "lon": 76.40},
    {"name": "Vellanikara", "lat": 10.54, "lon": 76.27},
    {"name": "Palakkad", "lat": 10.77, "lon": 76.65},
    {"name": "Pattambi", "lat": 10.81, "lon": 76.20},
    {"name": "Thavanur", "lat": 10.84, "lon": 76.00},
    {"name": "Tirur", "lat": 10.91, "lon": 75.92},
    {"name": "Kozhikode", "lat": 11.25, "lon": 75.77},
    {"name": "Kannur", "lat": 11.91, "lon": 75.36}
]

def get_ksdma_meta():
    """Scrapes KSDMA for 3-line Malayalam alert and official map links"""
    try:
        res = requests.get(KSDMA_URL, headers=HEADERS, verify=False, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        maps = {"max": None, "min": None, "humid": None}
        
        # Link Chasing for Maps
        max_l = soup.find('a', string=re.compile("Maximum Temperature", re.I))
        if max_l: maps["max"] = max_l.get('href') if max_l.get('href').startswith('http') else BASE_URL + max_l.get('href')
        
        min_l = soup.find('a', string=re.compile("Minimum Temperature", re.I))
        if min_l: maps["min"] = min_l.get('href') if min_l.get('href').startswith('http') else BASE_URL + min_l.get('href')

        humid_img = soup.find('img', src=re.compile("Hot-Humid", re.I))
        if humid_img: maps["humid"] = humid_img.get('src') if humid_img.get('src').startswith('http') else BASE_URL + humid_img.get('src')

        content = soup.find('div', class_='entry-content') or soup
        all_p = [p.get_text(strip=True) for p in content.find_all('p') if len(p.get_text()) > 30]
        
        return {
            "alert_header": "ഉയർന്ന താപനില മുന്നറിയിപ്പ് – മഞ്ഞ അലർട്ട്",
            "alert_paragraphs": all_p[:3],
            "max_map": maps["max"],
            "min_map": maps["min"],
            "humid_map": maps["humid"],
            "sync_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
    except: return {}

def get_imd_stations():
    """Fetches IMD scientific data and applies precise fuzzy naming"""
    try:
        res = requests.get(IMD_JSON_URL, verify=False, timeout=25)
        data = res.json()
        stations = []
        
        for feature in data.get('features', []):
            p = feature['properties']
            lon, lat = p.get('Longitude', 0), p.get('Latitude', 0)
            
            # Kerala Boundary Filter
            if 74.5 <= lon <= 77.5 and 8.0 <= lat <= 13.0:
                temp = p.get('D1F_Mx_Tem') or p.get('temp', 0)
                hum = p.get('D1_RH_0830') or p.get('rh', 0)
                dep = p.get('D1F_Mx_Dep', 0)

                # --- FUZZY NAME MATCHING ---
                matched_name = None
                for target in TARGETS:
                    # Match if within 0.1 degrees (~11km)
                    if abs(lat - target['lat']) < 0.1 and abs(lon - target['lon']) < 0.1:
                        matched_name = target['name']
                        break
                
                if not matched_name:
                    matched_name = p.get('Stat_Name') or p.get('District') or f"AWS {round(lat, 2)}"

                # HEAT STATUS LOGIC (Based on your Alert/Watch thresholds)
                status = "Normal"
                if temp >= 38: status = "Alert"
                elif temp >= 36: status = "Watch"

                # REAL FEEL (Heat Index)
                real_feel = temp
                if temp >= 27 and hum > 0:
                    real_feel = round(0.5 * (temp + 61.0 + ((temp - 68.0) * 1.2) + (hum * 0.094)), 1)

                stations.append({
                    "name": matched_name,
                    "lat": lat, "lon": lon,
                    "temp": temp,
                    "status": status,
                    "departure": f"+{dep}" if dep and dep > 0 else dep,
                    "humidity": hum,
                    "real_feel": real_feel,
                    "warning_code": p.get('warning_color', 4),
                    "sunrise": p.get('Sr_Time'),
                    "sunset": p.get('Ss_Time')
                })
        return stations
    except: return []

if __name__ == "__main__":
    print("🛰️ Starting Precise War Room Sync...")
    
    final_output = {
        "meta": get_ksdma_meta(),
        "stations": get_imd_stations()
    }

    os.makedirs('data', exist_ok=True)
    with open('data/weather.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)
        
    print(f"✅ Sync Successful. {len(final_output['stations'])} stations mapped.")
