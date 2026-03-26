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

# 2. PRECISE STATION NAME MAPPING (Matches your list exactly)
STATION_MAP = {
    "43371": "Thiruvananthapuram (Airport)",
    "43369": "Thiruvananthapuram",
    "43351": "Punalur",
    "43352": "Kottayam",
    "43355": "Kochi (CIAL)",
    "43356": "Vellanikara",
    "43357": "Palakkad",
    "43354": "Kozhikode",
    "43335": "Kannur",
    # AWS/District fallback mapping
    "Kollam": "Kollam",
    "Varkala": "Varkala",
    "Pattambi": "Pattambi",
    "Thavanur": "Thavanur",
    "Tirur": "Tirur"
}

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
    """Fetches IMD scientific data and applies precise naming and status alerts"""
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
                stat_id = str(p.get('Stat_Code'))
                raw_name = p.get('Stat_Name') or p.get('District') or ""

                # 1. APPLY PRECISE NAME
                name = STATION_MAP.get(stat_id)
                if not name:
                    for key in STATION_MAP:
                        if key.lower() in raw_name.lower():
                            name = STATION_MAP[key]
                            break
                if not name: name = raw_name if raw_name else f"AWS {lat}"

                # 2. DETERMINE HEAT STATUS (Alert/Watch/Normal)
                status = "Normal"
                if temp >= 38: status = "Alert"
                elif temp >= 36: status = "Watch"

                # 3. REAL FEEL (Heat Index)
                real_feel = temp
                if temp >= 27 and hum > 0:
                    real_feel = round(0.5 * (temp + 61.0 + ((temp - 68.0) * 1.2) + (hum * 0.094)), 1)

                stations.append({
                    "name": name,
                    "lat": lat, "lon": lon,
                    "temp": temp,
                    "status": status,
                    "departure": f"+{dep}" if dep and dep > 0 else dep,
                    "humidity": hum,
                    "real_feel": real_feel,
                    "rainfall": p.get('D1_Rainfall', 0),
                    "warning_code": p.get('warning_color', 4),
                    "sunrise": p.get('Sr_Time'),
                    "sunset": p.get('Ss_Time')
                })
        return stations
    except: return []

if __name__ == "__main__":
    print("🛰️ Starting Unified Kerala Heat Watch Sync...")
    
    final_output = {
        "meta": get_ksdma_meta(),
        "stations": get_imd_stations()
    }

    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    with open('data/weather.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)
        
    print(f"✅ Sync Successful. {len(final_output['stations'])} stations mapped.")
