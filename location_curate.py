import streamlit as st
import pandas as pd
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

# ---------------------------
# India-specific fallback data
# ---------------------------

DEFAULT_CITY_DATA = {
    'Delhi': {'state': 'Delhi', 'pincode': '110001', 'latitude': 28.6139, 'longitude': 77.2090},
    'Mumbai': {'state': 'Maharashtra', 'pincode': '400001', 'latitude': 18.9388, 'longitude': 72.8354},
    'Bengaluru': {'state': 'Karnataka', 'pincode': '560001', 'latitude': 12.9719, 'longitude': 77.5937},
    'Chennai': {'state': 'Tamil Nadu', 'pincode': '600001', 'latitude': 13.0827, 'longitude': 80.2707},
    'Hyderabad': {'state': 'Telangana', 'pincode': '500001', 'latitude': 17.3850, 'longitude': 78.4867},
    'Kolkata': {'state': 'West Bengal', 'pincode': '700001', 'latitude': 22.5726, 'longitude': 88.3639}
}
VALID_STATES = set([v['state'] for v in DEFAULT_CITY_DATA.values()])
VALID_CITIES = set(DEFAULT_CITY_DATA.keys())

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

# ---------------------------
# Provider-Specific Functions
# ---------------------------

def geocode_google(address, api_key):
    try:
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                           params={"address": address, "key": api_key})
        data = res.json()
        if data.get("status") == "OK":
            coords = data['results'][0]['geometry']['location']
            return coords['lat'], coords['lng'], True
    except:
        pass
    return None, None, False

def reverse_google(lat, lon, api_key):
    try:
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                           params={"latlng": f"{lat},{lon}", "key": api_key})
        components = res.json().get("results", [])[0].get("address_components", [])
        city, state, pin = None, None, None
        for comp in components:
            if "locality" in comp["types"] or "sublocality" in comp["types"]:
                city = comp["long_name"]
            if "administrative_area_level_1" in comp["types"]:
                state = comp["long_name"]
            if "postal_code" in comp["types"]:
                pin = comp["long_name"]
        return city, state, pin
    except:
        return None, None, None

def geocode_here(address, api_key):
    try:
        res = requests.get("https://geocode.search.hereapi.com/v1/geocode",
                           params={"q": address, "apiKey": api_key})
        data = res.json()
        if data.get("items"):
            coords = data["items"][0]["position"]
            return coords["lat"], coords["lng"], True
    except:
        pass
    return None, None, False

def reverse_here(lat, lon, api_key):
    try:
        res = requests.get("https://revgeocode.search.hereapi.com/v1/revgeocode",
                           params={"at": f"{lat},{lon}", "apiKey": api_key})
        info = res.json()["items"][0]["address"]
        return info.get("city"), info.get("state"), info.get("postalCode")
    except:
        return None, None, None

def geocode_mapbox(address, api_key):
    try:
        from urllib.parse import quote
        q = quote(address + ", India")
        res = requests.get(f"https://api.mapbox.com/geocoding/v5/mapbox.places/{q}.json",
                           params={"access_token": api_key})
        d = res.json()
        if d.get("features"):
            coords = d["features"][0]["geometry"]["coordinates"]
            return coords[1], coords[0], True
    except:
        pass
    return None, None, False

def reverse_mapbox(lat, lon, api_key):
    try:
        res = requests.get(f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json",
                           params={"access_token": api_key}).json()
        city, state, pin = None, None, None
        for feat in res.get("features", []):
            t = feat["place_type"]
            if "place" in t:
                city = feat["text"]
            if "region" in t:
                state = feat["text"]
            if "postcode" in t:
                pin = feat["text"]
        return city, state, pin
    except:
        return None, None, None

# ✅ Adding OLA Maps
def geocode_ola(address, api_key):
    try:
        res = requests.get("https://api.olamaps.io/places/v1/geocode",
                           params={"api_key": api_key, "address": address})
        d = res.json()
        if d.get('results'):
            loc = d["results"][0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"]), True
    except:
        pass
    return None, None, False

def reverse_ola(lat, lon, api_key):
    try:
        res = requests.get("https://api.olamaps.io/places/v1/reverse-geocode",
                           params={"api_key": api_key, "latlng": f"{lat},{lon}"})
        d = res.json()
        components = d.get("results", [])[0].get("address_components", {})
        return components.get("city"), components.get("state"), components.get("postal_code")
    except:
        return None, None, None

# ------------------------
# Address Enrichment Logic
# ------------------------

def enrich(row, address_col, provider, key):
    address = str(row.get(address_col, '')).strip()
    city = str(row.get("City", "")).strip()
    state = str(row.get("State", "")).strip()
    pin = str(row.get("Postal Code", "")).strip()
    lat = row.get("Latitude", None)
    lon = row.get("Longitude", None)
    notes = []
    used_fallback = False

    if not lat or not lon:
        if provider == "Google Maps":
            lat, lon, ok = geocode_google(address, key)
        elif provider == "HERE Maps":
            lat, lon, ok = geocode_here(address, key)
        elif provider == "Mapbox":
            lat, lon, ok = geocode_mapbox(address, key)
        elif provider == "OLA Maps":
            lat, lon, ok = geocode_ola(address, key)
        if ok:
            used_fallback = True
            notes.append("Lat/Lon enriched")

    city_ok = city in VALID_CITIES
    state_ok = state in VALID_STATES
    pin_ok = is_valid_indian_pincode(pin)

    if lat and lon and (not city_ok or not state_ok or not pin_ok):
        if provider == "Google Maps":
            r_city, r_state, r_pin = reverse_google(lat, lon, key)
        elif provider == "HERE Maps":
            r_city, r_state, r_pin = reverse_here(lat, lon, key)
        elif provider == "Mapbox":
            r_city, r_state, r_pin = reverse_mapbox(lat, lon, key)
        elif provider == "OLA Maps":
            r_city, r_state, r_pin = reverse_ola(lat, lon, key)

        if not city_ok and r_city:
            city = r_city
            notes.append("City from reverse")
        if not state_ok and r_state:
            state = r_state
            notes.append("State from reverse")
        if not pin_ok and is_valid_indian_pincode(r_pin):
            pin = r_pin
            notes.append("PIN from reverse")

    city, state, pin, lat, lon, fb_notes = apply_fallbacks(city, state, pin, lat, lon)
    notes += fb_notes

    return {
        "Latitude": lat, "Longitude": lon, "City": city, "State": state,
        "Postal Code": pin, "Used_Fallback": used_fallback,
        "Correction_Notes": ", ".join(notes),
        "Status": "Success" if lat and pin and city and state else "Failed"
    }

# ------------------------
# Streamlit App UI
# ------------------------

st.set_page_config(layout="wide")
st.title("📍 Indian Address Validator (Google, HERE, Mapbox, OLA Maps)")
provider = st.selectbox("🌐 Select Provider", ["Google Maps", "HERE Maps", "Mapbox", "OLA Maps"])
api_key = st.text_input(f"🔑 API Key for {provider}", type="password")
uploaded = st.file_uploader("📂 Upload Excel (.xlsx)", type=["xlsx"])
threads = st.slider("⚙️ Parallel Threads", 2, 20, 10)

if uploaded:
    df = pd.read_excel(uploaded)
    st.success("📄 File loaded successfully!")
    st.dataframe(df.head())
    address_col = st.selectbox("📬 Address column:", df.columns)

    if st.button("🚀 Start Enrichment"):
        st.info("⏳ Processing...")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(enrich, row, address_col, provider, api_key): i for i, row in df.iterrows()}
            progress = st.progress(0)
            results = [None] * len(df)
            for count, future in enumerate(as_completed(futures)):
                i = futures[future]
                results[i] = future.result()
                progress.progress((count + 1) / len(df))
        out_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(results)], axis=1)
        st.success("✅ Enrichment complete.")
        st.dataframe(out_df.head())

        output = BytesIO()
        out_df.to_excel(output, index=False)
        output.seek(0)
        st.download_button("📥 Download XLSX", data=output,
                           file_name="enriched_addresses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

