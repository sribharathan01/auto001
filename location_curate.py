import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# --- Default Fallback Data for Indian Cities ---
DEFAULT_CITY_DATA = {
    'Delhi': {'state': 'Delhi', 'pincode': '110001', 'latitude': 28.6139, 'longitude': 77.2090},
    'Mumbai': {'state': 'Maharashtra', 'pincode': '400001', 'latitude': 18.9388, 'longitude': 72.8354},
    'Bengaluru': {'state': 'Karnataka', 'pincode': '560001', 'latitude': 12.9719, 'longitude': 77.5937},
    'Chennai': {'state': 'Tamil Nadu', 'pincode': '600001', 'latitude': 13.0827, 'longitude': 80.2707},
    'Hyderabad': {'state': 'Telangana', 'pincode': '500001', 'latitude': 17.3850, 'longitude': 78.4867},
    'Kolkata': {'state': 'West Bengal', 'pincode': '700001', 'latitude': 22.5726, 'longitude': 88.3639},
}
VALID_STATES = set([c['state'] for c in DEFAULT_CITY_DATA.values()])
VALID_CITIES = set(DEFAULT_CITY_DATA.keys())

# --- Helpers ---
def is_valid_indian_pincode(pin):
    return bool(re.match(r'^[1-9][0-9]{5}$', str(pin).strip()))

def apply_fallbacks(city, state, pin, lat, lon):
    notes = []
    if city in DEFAULT_CITY_DATA:
        ref = DEFAULT_CITY_DATA[city]
        if not state:
            state = ref['state']
            notes.append("State from default")
        if not is_valid_indian_pincode(pin):
            pin = ref['pincode']
            notes.append("PIN from default")
        if not lat:
            lat = ref['latitude']
            notes.append("Lat from default")
        if not lon:
            lon = ref['longitude']
            notes.append("Lon from default")
    return city, state, pin, lat, lon, notes

# --- API: Google Maps ---
def geocode_google(address: str, api_key: str):
    try:
        params = {"address": address, "key": api_key}
        resp = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=params)
        data = resp.json()
        if data['status'] == 'OK':
            loc = data['results'][0]['geometry']['location']
            return loc['lat'], loc['lng'], True
    except Exception:
        pass
    return None, None, False

def reverse_google(lat, lon, api_key):
    try:
        params = {'latlng': f"{lat},{lon}", 'key': api_key}
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=params)
        info = res.json()
        components = info['results'][0]['address_components']
        city, state, pin = None, None, None
        for c in components:
            if 'locality' in c['types'] or 'sublocality' in c['types']:
                city = c['long_name']
            elif 'administrative_area_level_1' in c['types']:
                state = c['long_name']
            elif 'postal_code' in c['types']:
                pin = c['long_name']
        return city, state, pin
    except:
        return None, None, None

# --- API: HERE Maps ---
def geocode_here(address, api_key):
    try:
        url = "https://geocode.search.hereapi.com/v1/geocode"
        params = {"q": address, "apiKey": api_key}
        resp = requests.get(url, params=params).json()
        if 'items' in resp and resp['items']:
            pos = resp['items'][0]['position']
            return pos['lat'], pos['lng'], True
    except:
        pass
    return None, None, False

def reverse_here(lat, lon, api_key):
    try:
        url = "https://revgeocode.search.hereapi.com/v1/revgeocode"
        params = {"at": f"{lat},{lon}", "apiKey": api_key}
        d = requests.get(url, params=params).json()
        if d.get("items"):
            info = d["items"][0]["address"]
            return info.get("city"), info.get("state"), info.get("postalCode")
    except:
        return None, None, None
    return None, None, None

# --- API: Mapbox ---
def geocode_mapbox(address, token):
    try:
        from urllib.parse import quote
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{quote(address)}.json"
        res = requests.get(url, params={"access_token": token})
        data = res.json()
        if data.get("features"):
            coord = data['features'][0]['geometry']['coordinates']
            return coord[1], coord[0], True
    except:
        pass
    return None, None, False

def reverse_mapbox(lat, lon, token):
    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
        res = requests.get(url, params={"access_token": token}).json()
        city, state, pincode = None, None, None
        for feat in res.get("features", []):
            types = feat["place_type"]
            if "place" in types:
                city = city or feat["text"]
            if "region" in types:
                state = state or feat["text"]
            if "postcode" in types:
                pincode = pincode or feat["text"]
        return city, state, pincode
    except:
        return None, None, None

# --- API: OLA Maps (Placeholder) ---
def geocode_ola(address, api_key):
    # OLA API is not public—placeholder logic
    return None, None, False

def reverse_ola(lat, lon, api_key):
    # Placeholder reverse geocode
    return None, None, None

# --- Core Address Enrichment Function ---
def enrich_record(row, address_col, provider, key_info):
    address = str(row.get(address_col, '')).strip()
    city = str(row.get("City", "")).strip()
    state = str(row.get("State", "")).strip()
    pin = str(row.get("Postal Code", "")).strip()
    lat = row.get("Latitude", None)
    lon = row.get("Longitude", None)
    notes = []
    used_fallback = False

    # Step 1: Geocode if lat/lon missing
    if not lat or not lon:
        if provider == "Google Maps":
            lat, lon, success = geocode_google(address, key_info)
        elif provider == "HERE Maps":
            lat, lon, success = geocode_here(address, key_info)
        elif provider == "Mapbox":
            lat, lon, success = geocode_mapbox(address, key_info)
        elif provider == "OLA Maps":
            lat, lon, success = geocode_ola(address, key_info)
        else:
            success = False
        if success:
            used_fallback = True
            notes.append("Lat/Lon from geocoding")

    # Step 2: Reverse geocode for missing/invalid values
    city_ok = city in VALID_CITIES
    state_ok = state in VALID_STATES
    pin_ok = is_valid_indian_pincode(pin)

    if lat and lon and (not city_ok or not state_ok or not pin_ok):
        if provider == "Google Maps":
            r_city, r_state, r_pin = reverse_google(lat, lon, key_info)
        elif provider == "HERE Maps":
            r_city, r_state, r_pin = reverse_here(lat, lon, key_info)
        elif provider == "Mapbox":
            r_city, r_state, r_pin = reverse_mapbox(lat, lon, key_info)
        elif provider == "OLA Maps":
            r_city, r_state, r_pin = reverse_ola(lat, lon, key_info)
        else:
            r_city, r_state, r_pin = None, None, None
        if not city_ok and r_city:
            city = r_city
            notes.append("City from reverse")
        if not state_ok and r_state:
            state = r_state
            notes.append("State from reverse")
        if not pin_ok and is_valid_indian_pincode(r_pin):
            pin = r_pin
            notes.append("PIN from reverse")

    # Step 3: Apply static default
    city, state, pin, lat, lon, fallbacks = apply_fallbacks(city, state, pin, lat, lon)
    notes += fallbacks

    return {
        "Latitude": lat,
        "Longitude": lon,
        "City": city,
        "State": state,
        "Postal Code": pin,
        "Used_Fallback": used_fallback,
        "Correction_Notes": ", ".join(notes),
        "Status": "Success" if lat and city and state and pin else "Incomplete"
    }

# --- Parallel Processing ---
def process_data(df, address_col, provider, key, threads=10):
    output = [None] * len(df)
    with ThreadPoolExecutor(max_workers=threads) as exe:
        futures = {exe.submit(enrich_record, row, address_col, provider, key): idx for idx, row in df.iterrows()}
        progress = st.progress(0)
        for count, future in enumerate(as_completed(futures)):
            idx = futures[future]
            try:
                output[idx] = future.result()
            except Exception as e:
                output[idx] = {"Status": "Failed", "Error": str(e)}
            progress.progress((count + 1) / len(df))
    return pd.DataFrame(output)

# --- Streamlit UI ---
st.set_page_config(page_title="India Address Validator", layout="wide")
st.title("📍 India Address Validator & Enricher")
st.markdown("Supports Google Maps, Mapbox, HERE Maps, and OLA Maps (placeholder).")

provider = st.selectbox("🌐 Select Geocoding Provider", ["Google Maps", "Mapbox", "HERE Maps", "OLA Maps"])
api_key = st.text_input("🔑 Enter your API Key or Token", type="password")

uploaded_file = st.file_uploader("📤 Upload Excel file", type=["xlsx"])
threads = st.slider("⚙️ Threads", 2, 20, 10)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.success("Preview loaded")
    st.dataframe(df.head())
    address_col = st.selectbox("📌 Select address column", df.columns)

    if st.button("🚀 Start Enrichment"):
        st.info(f"Running with {provider}...")
        enriched = process_data(df, address_col, provider, api_key, threads)
        final = pd.concat([df.reset_index(drop=True), enriched], axis=1)

        st.success("🎉 Done! Preview below:")
        st.dataframe(final.head())

        output = BytesIO()
        final.to_excel(output, index=False)
        output.seek(0)
        st.download_button("⬇️ Download Enriched File", data=output,
                           file_name="enriched_addresses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
