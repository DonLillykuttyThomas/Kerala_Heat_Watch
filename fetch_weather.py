import requests
import json
import os
from datetime import datetime

def get_imd_dss_weather():
    # Official DSS Forecast Link (Status Code 200 verified)
    url = "https://dss.imd.gov.in/dwr_img/GIS/CD_Status_Forecast.json"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    # Precise Station Mapping from your IMD Directory list
    # Format: "Station_Code": ["Display Name", Latitude, Longitude]
    kerala_stations = {
        "43352": ["Alappuzha", 9.48, 76.31],
        "43315": ["Kannur", 11.86, 75.36],
        "43336": ["Kochi (CIAL)", 10.15, 76.40],
        "43355": ["Kottayam", 9.56, 76.56],
        "43314": ["Kozhikode", 11.25, 75.76],
        "43320": ["Karipur (Airport)", 11.13, 75.93],
        "43335": ["Palakkad", 10.83, 76.66],
        "43354": ["Punalur", 9.00, 76.93],
        "43371": ["Thiruvananthapuram City", 8.50, 76.95],
        "43372": ["Thiruvananthapuram (A)", 8.46, 76.95],
        "43357": ["Thrissur (Vellanikkara)", 10.53, 76.26]
    }

    try:
        # verify=False is required for IMD's SSL setup
        response = requests.get(url, headers=headers, timeout=25, verify=False)
        
        if response.status_code == 200:
            data = response.json()
            # GeoJSON structure: Data is in 'features' list
            features = data.get("features", [])
            final_results = {}

            for feature in features:
                # Attributes are stored in 'properties'
                p = feature.get("properties", {})
                stat_code = str(p.get("Stat_Code", ""))
                
                if stat_code in kerala_stations:
                    name = kerala_stations[stat_code][0]
                    # Extracting data based on verified Sample Properties
                    final_results[name] = {
                        "temp_max": p.get("D1F_Mx_Tem"), # Day 1 Forecast Max
                        "temp_min": p.get("D1F_Mn_Tem"), # Day 1 Forecast Min
                        "condition": p.get("D1F_Weathr", "Clear"), # Weather Desc
                        "humidity": p.get("D1_RH_0830"), # Morning Humidity
                        "warning_color": p.get("Day1_Color", "4"), # 1=Red, 2=Orange
                        "warning_code": p.get("Day1", "1"), # 9=Heatwave
                        "lat": kerala_stations[stat_code][1],
                        "lng": kerala_stations[stat_code][2],
                        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
            
            if final_results:
                print(f"✅ Success: Data captured for {len(final_results)} Kerala stations.")
            else:
                print("⚠️ Warning: No Kerala station codes found in the current feed.")
                
        else:
            print(f"❌ HTTP Error: {response.status_code}")

    except Exception as e:
        print(f"❌ Script Crash: {e}")
        final_results = {"error": str(e)}

    # Saving to the 'data' folder for your website
    os.makedirs('data', exist_ok=True)
    with open('data/weather.json', 'w') as f:
        json.dump(final_results, f, indent=4)

if __name__ == "__main__":
    get_imd_dss_weather()
