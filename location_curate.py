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

# --- Provider Functions ---
def geocode_google(address, api_key):
    params = {'address': address + ', India', 'key': api_key}
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=params)
        d = r.json()
        if d.get("status") == "OK":
            loc = d['results'][0]['geometry']['location']
            return loc['lat'], loc['lng'], True
    except:
        pass
    return None, None, False

def reverse_google(lat, lon, api_key):
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                         params={"latlng": f"{lat},{lon}", "key": api_key})
        data = r.json()
        city, state, pin = None, None, None
        for comp in data["results"][0]["address_components"]:
            types = comp["types"]
            if "locality" in types or "sublocality" in types:
                city = comp["long_name"]
            if "administrative_area_level_1" in types:
                state = comp["long_name"]
            if "postal_code" in types:
                pin = comp["long_name"]
        return city, state, pin
    except:
        return None, None, None

def geocode_mapbox(address, token):
    from urllib.parse import quote
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{quote(address + ', India')}.json"
    try:
        r = requests.get(url, params={"access_token": token})
        d = r.json()
        if d.get("features"):
            coords = d["features"][0]["geometry"]["coordinates"]
            return coords[1], coords[0], True
    except:
        pass
    return None, None, False

def reverse_mapbox(lat, lon, token):
    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
        r = requests.get(url, params={"access_token": token})
        data = r.json()
        city, state, pin = None, None, None
        for feature in data.get("features", []):
            pt = feature["place_type"]
            if 'place' in pt and not city:
                city = feature["text"]
            if 'region' in pt and not state:
                state = feature["text"]
            if 'postcode' in pt and not pin:
                pin = feature["text"]
        return city, state, pin
    except:
        return None, None, None

def geocode_here(address, api_key):
    url = "https://geocode.search.hereapi.com/v1/geocode"
    params = {'q': address + ', India', 'apiKey': api_key}
    try:
        r = requests.get(url, params=params)
        d = r.json()
        if d.get("items"):
            pos = d["items"][0]["position"]
            return pos["lat"], pos["lng"], True
    except:
        pass
    return None, None, False

def reverse_here(lat, lon, api_key):
    url = "https://revgeocode.search.hereapi.com/v1/revgeocode"
    params = {'at': f"{lat},{lon}", 'apiKey': api_key}
    try:
        r = requests.get(url, params=params)
        d = r.json()
        if d.get("items"):
            addr = d["items"][0]["address"]
            city = addr.get("city")
            state = addr.get("state")
            pin = addr.get("postalCode")
            return city, state, pin
    except:
        pass
    return None, None, None

def geocode_ola(address, api_key):
    # Placeholder: Replace with actual OLA Maps API when available
    return None, None, False

def reverse_ola(lat, lon, api_key):
    # Placeholder: Replace with actual OLA Maps API when available
    return None, None, None

# --- Main Enrichment Function ---
def validate_and_enrich(row, address_col, provider, credentials):
    address = str(row.get(address_col, '')).strip()
    city = str(row.get('City', '')).strip()
    state = str(row.get('State', '')).strip()
    pin = str(row.get('Postal Code', '')).strip()
    lat = row.get('Latitude', None)
    lon = row.get('Longitude', None)
    notes, used_fallback, used_defaults = [], False, False

    # Step 1: Geocode
    if not lat or not lon:
        if provider == "Google Maps":
            lat, lon, success = geocode_google(address, credentials['key'])
        elif provider == "Mapbox":
            lat, lon, success = geocode_mapbox(address, credentials['key'])
        elif provider == "HERE Maps":
            lat, lon, success = geocode_here(address, credentials['key'])
        elif provider == "OLA Maps":
            lat, lon, success = geocode_ola(address, credentials['key'])
        else:
            success = False
        if success:
            used_fallback = True
            notes.append("Lat/Lon from geocoding")

    # Step 2: Reverse Geocode if needed
    city_ok, state_ok, pin_ok = city in VALID_CITIES, state in VALID_STATES, is_valid_indian_pincode(pin)
    if lat and lon and (not city_ok or not state_ok or not pin_ok):
        if provider == "Google Maps":
            r_city, r_state, r_pin = reverse_google(lat, lon, credentials['key'])
        elif provider == "Mapbox":
            r_city, r_state, r_pin = reverse_mapbox(lat, lon, credentials['key'])
        elif provider == "HERE Maps":
            r_city, r_state, r_pin = reverse_here(lat, lon, credentials['key'])
        elif provider == "OLA Maps":
            r_city, r_state, r_pin = reverse_ola(lat, lon, credentials['key'])
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
def process_addresses(df, address_col, provider, credentials, max_threads):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(validate_and_enrich, row, address_col, provider, credentials): idx
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
st.title("India Address Validator")
st.markdown("""
Upload Excel 📄 > Select provider (Google Maps, Mapbox, HERE Maps, OLA Maps) > Enrich & Validate Indian address data.
""")

provider = st.selectbox("🌐 Select Geocoding Provider", [
    "Google Maps", "Mapbox", "HERE Maps", "OLA Maps"
])
credentials = {}
if provider in ["Google Maps", "Mapbox", "HERE Maps", "OLA Maps"]:
    credentials['key'] = st.text_input(f"{provider} API Key", type="password")

uploaded_file = st.file_uploader("📤 Upload your Excel file (.xlsx)", type=["xlsx"])
if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.success("✅ File uploaded. Preview:")
    st.dataframe(df.head())

    address_col = st.selectbox("📌 Select the address column:", df.columns)
    threads = st.slider("⚙️ Number of Parallel Threads", 2, 20, value=10)

    if st.button("🚀 Start processing"):
        st.info("Processing... please wait ⏳")
        result_df = process_addresses(
            df, address_col, provider, credentials, threads
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
