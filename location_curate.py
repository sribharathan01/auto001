import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from io import BytesIO

# --- HELPERS ---
def is_valid_indian_pincode(pin):
    return bool(re.match(r'^[1-9][0-9]{5}$', str(pin)))

def google_geocode_address(address, api_key):
    try:
        params = {'address': address, 'key': api_key}
        response = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
        data = response.json()
        if data.get('status') == 'OK':
            location = data['results'][0]['geometry']['location']
            return location.get('lat'), location.get('lng'), True
    except Exception:
        pass
    return None, None, False

def google_reverse_geocode(lat, lon, api_key):
    """Returns (city, state, postal_code) from lat/lon via reverse geocoding"""
    try:
        params = {'latlng': f'{lat},{lon}', 'key': api_key}
        response = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
        data = response.json()
        if data.get('status') == 'OK':
            components = data['results'][0]['address_components']
            city = state = postal_code = None
            for comp in components:
                if 'locality' in comp['types']:
                    city = comp['long_name']
                if 'administrative_area_level_1' in comp['types']:
                    state = comp['long_name']
                if 'postal_code' in comp['types']:
                    postal_code = comp['long_name']
            return city, state, postal_code
    except Exception:
        pass
    return None, None, None

def validate_address(address, api_key, retries=3):
    endpoint = f'https://addressvalidation.googleapis.com/v1:validateAddress?key={api_key}'
    payload = {
        "address": {
            "regionCode": "IN",
            "addressLines": [address]
        }
    }
    for attempt in range(retries):
        try:
            response = requests.post(endpoint, json=payload)
            data = response.json()

            if response.status_code == 429:
                time.sleep(5)
                continue
            elif response.status_code != 200:
                return {'Status': 'Failed', 'Error': f"HTTP {response.status_code}"}

            addr_info = data.get('result', {}).get('address', {})
            location = addr_info.get('location', {})
            postal = addr_info.get('postalAddress', {})

            lat = location.get('latitude')
            lon = location.get('longitude')
            used_fallback = False

            if lat is None or lon is None:
                lat, lon, used_fallback = google_geocode_address(address, api_key)

            city = postal.get('locality')
            state = postal.get('administrativeArea')
            zipcode = postal.get('postalCode')

            # --- REVERSE GEOCODE IF ANY FIELD IS MISSING ---
            if (not is_valid_indian_pincode(zipcode)) or not city or not state:
                if lat and lon:
                    city_rev, state_rev, postal_code_rev = google_reverse_geocode(lat, lon, api_key)

                    city = city or city_rev
                    state = state or state_rev
                    if not is_valid_indian_pincode(zipcode):
                        zipcode = postal_code_rev if is_valid_indian_pincode(postal_code_rev) else None

                    status = 'Reverse Geocoding Fallback Used'
                else:
                    status = 'Missing Location Info'
            else:
                status = 'Success'

            if used_fallback:
                status += ' (Lat/Lon Fallback Used)'

            return {
                'City': city,
                'State': state,
                'Postal Code': zipcode,
                'Latitude': lat,
                'Longitude': lon,
                'Used_Fallback_Geocoding': used_fallback,
                'Status': status
            }

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return {
                    'City': None,
                    'State': None,
                    'Postal Code': None,
                    'Latitude': None,
                    'Longitude': None,
                    'Used_Fallback_Geocoding': False,
                    'Status': 'Failed',
                    'Error': str(e)
                }

def process_row(row, address_column, api_key):
    address = str(row[address_column])
    return validate_address(address, api_key)

def process_addresses(df, address_column, api_key, max_workers=10):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_row, row, address_column, api_key): idx
            for idx, row in df.iterrows()
        }
        progress = st.progress(0)
        completed = 0
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {'Status': 'Failed', 'Error': str(exc)}
            results[idx] = result
            completed += 1
            progress.progress(completed / len(df))
    return results

# --- STREAMLIT UI ---
st.title("🗺️ Bulk Address Validation, Geocoding & Reverse Cleanup")

api_key = st.text_input("AIzaSyBfDa2M6G1JL5kHImqfs9517i6g_9KXwvc", type="password")

st.markdown("""
Upload a file with addresses, and we’ll enrich missing City, State, Postal Code using:
- 🔹 Address Validation API
- 🔹 Geocoding fallback
- 🔹 Full reverse geocoding cleanup  
""")

uploaded_file = st.file_uploader("📎 Upload Excel File", type=["xlsx"])
address_column = st.text_input("🔤 Column containing addresses", value="Address")
max_workers = st.slider("⚙️ Parallel Threads", min_value=1, max_value=20, value=10)

if uploaded_file:
    if not api_key:
        st.warning("Please enter your Google API key to continue.")
    else:
        df = pd.read_excel(uploaded_file)
        st.info(f"Loaded {len(df)} rows.")
        st.write("📌 Preview of your data:", df.head())

        if st.button("🚀 Start Processing"):
            st.info("Processing addresses...")
            results = process_addresses(df, address_column, api_key, max_workers=max_workers)
            results_df = pd.DataFrame(results)
            final_df = pd.concat([df, results_df], axis=1)
            st.success("✅ Address processing complete.")

            st.write("📄 Preview of Results:", final_df.head())

            buffer = BytesIO()
            final_df.to_excel(buffer, index=False)
            buffer.seek(0)

            st.download_button(
                label="⬇️ Download Enriched Excel",
                data=buffer,
                file_name="validated_addresses_output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
