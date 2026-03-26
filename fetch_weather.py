import requests
import json
import os
from datetime import datetime

def get_dss_weather():
    url = "https://dss.imd.gov.in/dwr_img/GIS/CD_Status_Forecast.json"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print("🛰️ Opening the 'features' drawer in DSS...")

    try:
        response = requests.get(url, headers=headers, timeout=20, verify=False)
        
        if response.status_code == 200:
            data = response.json()
            
            # The data we want is inside the 'features' key
            all_features = data.get("features", [])
            kerala_data = {}

            for feature in all_features:
                # In GeoJSON, attributes are usually inside 'properties'
                props = feature.get("properties", {})
                
                if props.get("State") == "KERALA":
                    district = props.get("District")
                    kerala_data[district] = {
                        "status": props.get("Status", "Normal"),
                        "day1_color": props.get("Day1_Color", "4"), # 1=Red, 2=Orange, 3=Yellow, 4=Green
                        "warning_code": props.get("Day1", "1"),    # 9=Heat Wave, 10=Hot Day
                        "temp_max": props.get("Today_Max_temp", "N/A"),
                        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
            
            if kerala_data:
                print(f"✅ Found {len(kerala_data)} Kerala districts!")
            else:
                print("⚠️ Kerala not found inside 'features'. checking property names...")
        else:
            print(f"❌ Server error: {response.status_code}")

    except Exception as e:
        print(f"❌ Script Error: {e}")
        kerala_data = {"error": str(e)}

    os.makedirs('data', exist_ok=True)
    with open('data/weather.json', 'w') as f:
        json.dump(kerala_data, f, indent=4)

if __name__ == "__main__":
    get_dss_weather()
