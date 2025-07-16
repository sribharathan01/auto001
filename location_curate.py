# -- Existing imports
import streamlit as st
import pandas as pd
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

# -------------------- Defaults --------------------
DEFAULT_CITY_DATA = {
    'Delhi':       {'state': 'Delhi', 'pincode': '110001', 'latitude': 28.6139, 'longitude': 77.2090},
    'Mumbai':      {'state': 'Maharashtra', 'pincode': '400001', 'latitude': 18.9388, 'longitude': 72.8354},
    'Bengaluru':   {'state': 'Karnataka', 'pincode': '560001', 'latitude': 12.9719, 'longitude': 77.5937},
    'Chennai':     {'state': 'Tamil Nadu', 'pincode': '600001', 'latitude': 13.0827, 'longitude': 80.2707},
    'Hyderabad':   {'state': 'Telangana', 'pincode': '500001', 'latitude': 17.3850, 'longitude': 78.4867},
    'Kolkata':     {'state': 'West Bengal', 'pincode': '700001', 'latitude': 22.5726, 'longitude': 88.3639}
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
            notes.append("Default State")
        if not is_valid_indian_pincode(pin):
            pin = ref['pincode']
            notes.append("Default PIN")
        if not lat:
            lat = ref['latitude']
            notes.append("Default Lat")
        if not lon:
            lon = ref['longitude']
            notes.append("Default Lon")
    return city, state, pin, lat, lon, notes

# -------------------- Provider APIs --------------------

# -- Google
def geocode_google(address, key):
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                         params={"address": address, "key": key})
        d = r.json()
        if d.get("results"):
            loc = d["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"], True
    except: pass
    return None, None, False

def reverse_google(lat, lon, key):
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                         params={"latlng": f"{lat},{lon}", "key": key})
        comps = r.json().get("results", [])[0].get("address_components", [])
        city, state, pin = None, None, None
        for c in comps:
            if "locality" in c["types"] or "sublocality" in c["types"]:
                city = c["long_name"]
            if "administrative_area_level_1" in c["types"]:
                state = c["long_name"]
            if "postal_code" in c["types"]:
                pin = c["long_name"]
        return city, state, pin
    except: return None, None, None

# -- HERE
def geocode_here(address, key):
    try:
        r = requests.get("https://geocode.search.hereapi.com/v1/geocode",
                         params={"q": address, "apiKey": key})
        d = r.json()
        if d.get("items"):
            pos = d["items"][0]["position"]
            return pos["lat"], pos["lng"], True
    except: pass
    return None, None, False

def reverse_here(lat, lon, key):
    try:
        r = requests.get("https://revgeocode.search.hereapi.com/v1/revgeocode",
                         params={"at": f"{lat},{lon}", "apiKey": key})
        addr = r.json()["items"][0]["address"]
        return addr.get("city"), addr.get("state"), addr.get("postalCode")
    except: return None, None, None

# -- Mapbox
def geocode_mapbox(address, key):
    try:
        from urllib.parse import quote
        q = quote(address + ", India")
        r = requests.get(f"https://api.mapbox.com/geocoding/v5/mapbox.places/{q}.json",
                         params={"access_token": key})
        data = r.json()
        if data.get("features"):
            coords = data["features"][0]["geometry"]["coordinates"]
            return coords[1], coords[0], True
    except: pass
    return None, None, False

def reverse_mapbox(lat, lon, key):
    try:
        r = requests.get(f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json",
                         params={"access_token": key}).json()
        city, state, pin = None, None, None
        for f in r.get("features", []):
            pt = f["place_type"]
            if "place" in pt and not city:
                city = f["text"]
            if "region" in pt and not state:
                state = f["text"]
            if "postcode" in pt and not pin:
                pin = f["text"]
        return city, state, pin
    except: return None, None, None

# -- Ola Maps
def geocode_ola(address, key):
    try:
        r = requests.get("https://api.olamaps.io/places/v1/geocode",
                         params={"api_key": key, "address": address})
        data = r.json()
        if data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"]), True
    except: pass
    return None, None, False

def reverse_ola(lat, lon, key):
    try:
        r = requests.get("https://api.olamaps.io/places/v1/reverse-geocode",
                         params={"api_key": key, "latlng": f"{lat},{lon}"})
        data = r.json()
        comp = data.get("results", [])[0].get("address_components", {})
        return comp.get("city"), comp.get("state"), comp.get("postal_code")
    except: return None, None, None

# ✅ OpenCage
def geocode_opencage(address, key):
    try:
        r = requests.get("https://api.opencagedata.com/geocode/v1/json",
                         params={"q": f"{address}, India", "key": key})
        d = r.json()
        if d.get("results"):
            g = d["results"][0]["geometry"]
            return g["lat"], g["lng"], True
    except: pass
    return None, None, False

def reverse_opencage(lat, lon, key):
    try:
        r = requests.get("https://api.opencagedata.com/geocode/v1/json",
                         params={"q": f"{lat},{lon}", "key": key})
        comps = r.json()["results"][0]["components"]
        city = comps.get("city") or comps.get("town") or comps.get("village")
        return city, comps.get("state"), comps.get("postcode")
    except: return None, None, None

# ------------------------- Enrichment ------------------------

def enrich(row, address_col, provider, key):
    address = str(row.get(address_col, '')).strip()
    city = str(row.get("City", "")).strip()
    state = str(row.get("State", "")).strip()
    pin = str(row.get("Postal Code", "")).strip()
    lat = row.get("Latitude", None)
    lon = row.get("Longitude", None)
    notes, used_fallback = [], False

    if not lat or not lon:
        if provider == "Google Maps":
            lat, lon, ok = geocode_google(address, key)
        elif provider == "HERE Maps":
            lat, lon, ok = geocode_here(address, key)
        elif provider == "Mapbox":
            lat, lon, ok = geocode_mapbox(address, key)
        elif provider == "OLA Maps":
            lat, lon, ok = geocode_ola(address, key)
        elif provider == "OpenCage":
            lat, lon, ok = geocode_opencage(address, key)
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
        elif provider == "OpenCage":
            r_city, r_state, r_pin = reverse_opencage(lat, lon, key)

        if not city_ok and r_city:
            city = r_city
            notes.append("City from reverse")
        if not state_ok and r_state:
            state = r_state
            notes.append("State from reverse")
        if not pin_ok and is_valid_indian_pincode(r_pin):
            pin = r_pin
            notes.append("PIN from reverse")

    city, state, pin, lat, lon, fallback_notes = apply_fallbacks(city, state, pin, lat, lon)
    notes += fallback_notes

    return {
        "Latitude": lat, "Longitude": lon, "City": city, "State": state,
        "Postal Code": pin, "Used_Fallback": used_fallback,
        "Correction_Notes": ", ".join(notes),
        "Status": "Success" if all([lat, lon, city, state, pin]) else "Failed"
    }

# ------------------------- UI ------------------------

st.set_page_config(layout="wide")
st.title("📍 Address Validator with Google, HERE, Mapbox, Ola, OpenCage")
provider = st.selectbox("🌎 Select Provider", ["Google Maps", "HERE Maps", "Mapbox", "OLA Maps", "OpenCage"])
api_key = st.text_input(f"🔐 API Key for {provider}", type="password")
file = st.file_uploader("📂 Upload Excel File", type=["xlsx"])
threads = st.slider("⚙️ Threads", 2, 20, 10)

if file:
    df = pd.read_excel(file)
    st.success("✅ File loaded")
    st.dataframe(df.head())
    addr_col = st.selectbox("🏷️ Select Address Column", df.columns)

    if st.button("🚀 Start Enrichment"):
        st.info("🔄 Processing...")
        results = [None] * len(df)
        with ThreadPoolExecutor(max_workers=threads) as exe:
            futures = {exe.submit(enrich, r, addr_col, provider, api_key): i for i, r in df.iterrows()}
            progress = st.progress(0.0)
            for count, future in enumerate(as_completed(futures)):
                idx = futures[future]
                results[idx] = future.result()
                progress.progress((count + 1) / len(df))

        result_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(results)], axis=1)
        st.success("✅ Enrichment Complete")

        # ✅ Output Preview!
        st.subheader("📄 Preview of Enriched Output")
        st.dataframe(result_df.head())
        st.write(f"👁️ Showing 5 of {len(result_df)} records")

        output = BytesIO()
        result_df.to_excel(output, index=False)
        output.seek(0)
        st.download_button("📥 Download Enriched Excel", data=output,
                           file_name="enriched_addresses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
