import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# --- India Reference Data (expand as needed) ---
DEFAULT_CITY_DATA = {
    'Delhi': {'state': 'Delhi', 'pincode': '110001', 'latitude': 28.6139, 'longitude': 77.2090},
    'Mumbai': {'state': 'Maharashtra', 'pincode': '400001', 'latitude': 18.9388, 'longitude': 72.8354},
    'Bengaluru': {'state': 'Karnataka', 'pincode': '560001', 'latitude': 12.9719, 'longitude': 77.5937},
    'Chennai': {'state': 'Tamil Nadu', 'pincode': '600001', 'latitude': 13.0827, 'longitude': 80.2707},
    'Hyderabad': {'state': 'Telangana', 'pincode': '500001', 'latitude': 17.3850, 'longitude': 78.4867},
    'Kolkata': {'state': 'West Bengal', 'pincode': '700001', 'latitude': 22.5726, 'longitude': 88.3639}
}
VALID_STATES = set(val['state'] for val in DEFAULT_CITY_DATA.values())
VALID_CITIES = set(DEFAULT_CITY_DATA.keys())

def is_valid_indian_pincode(pin):
    return bool(re.match(r'^[1-9][0-9]{5}$', str(pin).strip()))

def apply_default_values(city, state, pincode, lat, lon):
    notes = []
    if city in DEFAULT_CITY_DATA:
        defaults = DEFAULT_CITY_DATA[city]
        if not state:
            state = defaults['state']
            notes.append("Default state")
        if not is_valid_indian_pincode(pincode):
            pincode = defaults['pincode']
            notes.append("Default PIN")
        if not lat:
            lat = defaults['latitude']
            notes.append("Default latitude")
        if not lon:
            lon = defaults['longitude']
            notes.append("Default longitude")
    return city, state, pincode, lat, lon, notes

# --- Nominatim (OpenStreetMap) ---
def geocode_nominatim(address, user_agent):
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address + ", India", "format": "json", "addressdetails": 1}
        headers = {"User-Agent": user_agent}
        r = requests.get(url, params=params, headers=headers)
        d = r.json()
        if d:
            return float(d[0]['lat']), float(d[0]['lon']), True
    except:
        pass
    return None, None, False

def reverse_nominatim(lat, lon, user_agent):
    try:
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {"lat": lat, "lon": lon, "format": "json", "addressdetails": 1}
        headers = {"User-Agent": user_agent}
        r = requests.get(url, params=params, headers=headers)
        d = r.json()
        addr = d.get("address", {})
        city = addr.get("city") or addr.get("town") or addr.get("village")
        state = addr.get("state")
        pin = addr.get("postcode")
        return city, state, pin
    except:
        return None, None, None

# --- OpenCage ---
def geocode_opencage(address, api_key):
    try:
        url = "https://api.opencagedata.com/geocode/v1/json"
        params = {"q": address + ", India", "key": api_key}
        r = requests.get(url, params=params)
        d = r.json()
        if d.get("results"):
            coords = d["results"][0]["geometry"]
            return coords["lat"], coords["lng"], True
    except:
        pass
    return None, None, False

def reverse_opencage(lat, lon, api_key):
    try:
        url = "https://api.opencagedata.com/geocode/v1/json"
        params = {"q": f"{lat},{lon}", "key": api_key}
        r = requests.get(url, params=params)
        d = r.json()
        if d.get("results"):
            comp = d["results"][0]["components"]
            city = comp.get("city") or comp.get("town") or comp.get("village")
            state = comp.get("state")
            pin = comp.get("postcode")
            return city, state, pin
    except:
        return None, None, None

# --- Offline PIN code lookup (expand with your CSV/JSON) ---
def lookup_pin_offline(pin, pin_df):
    try:
        row = pin_df[pin_df['pincode'] == str(pin)].iloc[0]
        return row['city'], row['state'], row['latitude'], row['longitude']
    except:
        return None, None, None, None

# --- Main Enrichment Function ---
def validate_and_enrich(row, address_col, provider, credentials, pin_df=None):
    address = str(row.get(address_col, '')).strip()
    city = str(row.get('City', '')).strip()
    state = str(row.get('State', '')).strip()
    pin = str(row.get('Postal Code', '')).strip()
    lat = row.get('Latitude', None)
    lon = row.get('Longitude', None)
    notes, used_fallback, used_defaults = [], False, False

    # Step 1: Geocode
    if not lat or not lon:
        if provider == "Nominatim":
            lat, lon, success = geocode_nominatim(address, credentials['user_agent'])
        elif provider == "OpenCage":
            lat, lon, success = geocode_opencage(address, credentials['key'])
        elif provider == "Offline":
            if is_valid_indian_pincode(pin) and pin_df is not None:
                city, state, lat, lon = lookup_pin_offline(pin, pin_df)
                success = bool(lat and lon)
            else:
                success = False
        else:
            success = False
        if success:
            used_fallback = True
            notes.append("Lat/Lon from geocoding")

    # Step 2: Reverse Geocode if needed
    city_ok, state_ok, pin_ok = city in VALID_CITIES, state in VALID_STATES, is_valid_indian_pincode(pin)
    if lat and lon and (not city_ok or not state_ok or not pin_ok):
        if provider == "Nominatim":
            r_city, r_state, r_pin = reverse_nominatim(lat, lon, credentials['user_agent'])
        elif provider == "OpenCage":
            r_city, r_state, r_pin = reverse_opencage(lat, lon, credentials['key'])
        elif provider == "Offline" and pin_df is not None:
            r_city, r_state, _, _ = lookup_pin_offline(pin, pin_df)
            r_pin = pin
        else:
            r_city, r_state, r_pin = None, None, None

        if not city_ok and r_city in VALID_CITIES:
            city = r_city
            notes.append("City from reverse")
        if not state_ok and r_state in VALID_STATES:
            state = r_state
            notes.append("State from reverse")
        if not pin_ok and is_valid_indian_pincode(r_pin):
            pin = r_pin
            notes.append("PIN from reverse")

    # Step 3: Apply defaults
    city, state, pin, lat, lon, fallback_notes = apply_default_values(city, state, pin, lat, lon)
    if fallback_notes:
        notes += fallback_notes
        used_defaults = True

    return {
        "Latitude": lat,
        "Longitude": lon,
        "Postal Code": pin,
        "City": city,
        "State": state,
        "Used_Fallback": used_fallback,
        "Used_Default": used_defaults,
        "Correction_Notes": ", ".join(notes) if notes else "No changes",
        "Status": "Success" if lat and lon and city and state and pin else "Incomplete"
    }

# --- Parallel Processing ---
def process_addresses(df, address_col, provider, credentials, max_threads, pin_df=None):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(validate_and_enrich, row, address_col, provider, credentials, pin_df): idx
            for idx, row in df.iterrows()
        }
        progress = st.progress(0)
        for count, future in enumerate(as_completed(futures)):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {"Status": "Failed", "Error": str(e)}
            progress.progress((count + 1) / len(df))
    return pd.DataFrame(results)

# --- Streamlit UI ---
st.title("🇮🇳 India Address Validator (Nominatim, OpenCage, Offline)")
st.markdown("""
Upload Excel 📄 > Select provider (Nominatim / OpenCage / Offline) > Enrich & Validate Indian address data.
""")

provider = st.selectbox("🌐 Select Geocoding Provider", ["Nominatim", "OpenCage", "Offline"])
credentials = {}
if provider == "Nominatim":
    credentials['user_agent'] = st.text_input("Nominatim User-Agent (required)", value="my-app")
elif provider == "OpenCage":
    credentials['key'] = st.text_input("OpenCage API Key", type="password")
elif provider == "Offline":
    pin_file = st.file_uploader("Upload Indian PIN code CSV (pincode,city,state,latitude,longitude)", type=["csv"])
    pin_df = pd.read_csv(pin_file) if pin_file else None
else:
    pin_df = None

uploaded_file = st.file_uploader("📤 Upload your Excel file (.xlsx)", type=["xlsx"])
if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.success("✅ File uploaded. Preview:")
    st.dataframe(df.head())

    address_col = st.selectbox("📌 Select the address column:", df.columns)
    threads = st.slider("⚙️ Number of Parallel Threads", 2, 20, value=10)

    if st.button("🚀 Start Enrichment"):
        st.info("Processing... please wait ⏳")
        result_df = process_addresses(
            df, address_col, provider, credentials, threads,
            pin_df if provider == "Offline" else None
        )
        final_df = pd.concat([df.reset_index(drop=True), result_df], axis=1)
        st.success("🎉 Done!")
        st.dataframe(final_df.head())

        out = BytesIO()
        final_df.to_excel(out, index=False)
        out.seek(0)
        st.download_button("⬇️ Download Results", data=out,
                           file_name="indian_enriched_addresses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
