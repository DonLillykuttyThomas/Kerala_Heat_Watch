import requests
from bs4 import BeautifulSoup
import json
import re
import urllib3
from datetime import datetime
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
KSDMA_URL = "https://sdma.kerala.gov.in/temperature/"
IMD_JSON_URL = "https://dss.imd.gov.in/dwr_img/GIS/CD_Status_Forecast.json"
BASE_URL = "https://sdma.kerala.gov.in"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def get_ksdma_meta():
    try:
        res = requests.get(KSDMA_URL, headers=HEADERS, verify=False, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        maps = {"max": None, "min": None, "humid": None}
        
        # Link Chasing
        max_l = soup.find('a', string=re.compile("Maximum Temperature", re.I))
        if max_l: maps["max"] = max_l.get('href') if max_l.get('href').startswith('http') else BASE_URL + max_l.get('href')
        
        min_l = soup.find('a', string=re.compile("Minimum Temperature", re.I))
        if min_l: maps["min"] = min_l.get('href') if min_l.get('href').startswith('http') else BASE_URL + min_l.get('href')

        humid_img = soup.find('img', src=re.compile("Hot-Humid", re.I))
        if humid_img: maps["humid"] = humid_img.get('src') if humid_img.get('src').startswith('http') else BASE_URL + humid_img.get('src')

        content = soup.find('div', class_='entry-content') or soup
        all_p = [p.get_text(strip=True) for p in content.find_all('p') if len(p.get_text()) > 25]
        
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
    try:
        res = requests.get(IMD_JSON_URL, verify=False, timeout=25)
        data = res.json()
        stations = []
        for feature in data.get('features', []):
            p = feature['properties']
            lon, lat = p.get('Longitude', 0), p.get('Latitude', 0)
            
            # Boundary Check for Kerala
            if 74.5 <= lon <= 77.5 and 8.0 <= lat <= 13.0:
                temp = p.get('D1F_Mx_Tem')
                hum = p.get('D1_RH_0830')
                dep = p.get('D1F_Mx_Dep')
                rain = p.get('D1_Rainfall', 0)
                
                # Real Feel Calculation
                real_feel = temp
                if temp and hum and temp >= 27:
                    real_feel = round(0.5 * (temp + 61.0 + ((temp - 68.0) * 1.2) + (hum * 0.094)), 1)

                stations.append({
                    "name": p.get('Stat_Name'),
                    "lat": lat, "lon": lon,
                    "temp": temp,
                    "departure": f"+{dep}" if dep and dep > 0 else dep,
                    "humidity": hum,
                    "real_feel": real_feel,
                    "rainfall": rain if rain else 0,
                    "warning_code": p.get('warning_color', 4), # 1:Red, 2:Orange, 3:Yellow, 4:Green
                    "sunrise": p.get('Sr_Time'), "sunset": p.get('Ss_Time')
                })
        return stations
    except: return []

if __name__ == "__main__":
    final_data = {"meta": get_ksdma_meta(), "stations": get_imd_stations()}
    os.makedirs('data', exist_ok=True)
    with open('data/weather.json', 'w', encoding='utf-8') as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)
    print("✅ Sync Complete.")
