import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# ---------------------------------------------
# INDIA VALIDATION - Defaults, States, Cities
# ---------------------------------------------
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
    return city, state, pincode, lat, lon, []

# ---------------------------------------------
# GEO APIs
# ---------------------------------------------
def geocode_address_google(address, api_key):
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

def geocode_address_mapbox(address, api_key):
    from urllib.parse import quote
    query = quote(address + ", India")
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"
    try:
        r = requests.get(url, params={"access_token": api_key})
        d = r.json()
        if d.get("features"):
            coords = d["features"][0]["geometry"]["coordinates"]  # [lon, lat]
            return coords[1], coords[0], True
    except:
        pass
    return None, None, False

def reverse_geocode_google(lat, lon, api_key):
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                         params={"latlng": f"{lat},{lon}", "key": api_key})
        d = r.json()
        if d.get("status") == "OK":
            city, state, pin = None, None, None
            for comp in d["results"][0]["address_components"]:
                types = comp["types"]
                if "locality" in types or "sublocality" in types:
                    city = comp["long_name"]
                if "administrative_area_level_1" in types:
                    state = comp["long_name"]
                if "postal_code" in types:
                    pin = comp["long_name"]
            return city, state, pin
    except:
        pass
    return None, None, None

def reverse_geocode_mapbox(lat, lon, api_key):
    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
        r = requests.get(url, params={"access_token": api_key})
        d = r.json()
        city, state, pin = None, None, None
        for feat in d.get("features", []):
            types = feat["place_type"]
            if "place" in types and not city:
                city = feat["text"]
            if "region" in types and not state:
                state = feat["text"]
            if "postcode" in types and not pin:
                pin = feat["text"]
        return city, state, pin
    except:
        pass
    return None, None, None

# ---------------------------------------------
# ENRICHMENT FUNCTION
# ---------------------------------------------
def validate_and_enrich(row, address_column, provider, api_key):
    address = str(row.get(address_column, "")).strip()
    city = str(row.get("City", "")).strip()
    state = str(row.get("State", "")).strip()
    pin = str(row.get("Postal Code", "")).strip()
    lat = row.get("Latitude", None)
    lon = row.get("Longitude", None)

    notes = []
    used_fallback = False

    # Geocode if needed
    if not lat or not lon:
        if provider == "Google Maps":
            lat, lon, success = geocode_address_google(address, api_key)
        else:
            lat, lon, success = geocode_address_mapbox(address, api_key)
        if success:
            used_fallback = True
            notes.append("Lat/Lon from geocode")

    # Reverse geocode if needed
    if lat and lon:
        if provider == "Google Maps":
            r_city, r_state, r_pin = reverse_geocode_google(lat, lon, api_key)
        else:
            r_city, r_state, r_pin = reverse_geocode_mapbox(lat, lon, api_key)

        if not city and r_city:
            city = r_city
            notes.append("City from reverse")
        if not state and r_state:
            state = r_state
            notes.append("State from reverse")
        if not pin or not is_valid_indian_pincode(pin):
            if is_valid_indian_pincode(r_pin):
                pin = r_pin
                notes.append("PIN from reverse")

    # Final fallback: defaults
    city, state, pin, lat, lon, default_notes = apply_default_values(city, state, pin, lat, lon)
    notes += default_notes
    used_defaults = bool(default_notes)

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

# ---------------------------------------------
# PARALLEL PROCESSING
# ---------------------------------------------
def process_addresses(df, address_col, provider, api_key, max_threads):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(validate_and_enrich, row, address_col, provider, api_key): idx
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

# ---------------------------------------------
# STREAMLIT UI
# ---------------------------------------------
st.title("🇮🇳 India Address Validator")
st.markdown("""
Upload Excel 📄 > Select provider (Google Maps / Mapbox) > Enrich & Validate Indian address data.
""")

provider = st.selectbox("🌐 Select Geocoding Provider", ["Google Maps", "Mapbox"])
api_key = st.text_input("🔑 Enter your API Key / Token", type="password")

if not api_key:
    st.warning("No API key provided.")
    st.stop()

uploaded_file = st.file_uploader("📂 Upload your Excel file (.xlsx)", type=["xlsx"])
if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.success("✅ File uploaded. Preview:")
    st.dataframe(df.head())

    address_col = st.selectbox("📌 Select the address column:", df.columns)
    threads = st.slider("⚙️ Number of Parallel Threads", 2, 20, value=10)

    if st.button("🚀 Start Enrichment"):
        st.info("Processing... please wait ⏳")
        enriched_df = process_addresses(df, address_col, provider, api_key, threads)
        final_df = pd.concat([df.reset_index(drop=True), enriched_df], axis=1)
        st.success("🎉 Done!")
        st.dataframe(final_df.head())

        # Download
        out = BytesIO()
        final_df.to_excel(out, index=False)
        out.seek(0)
        st.download_button("⬇️ Download Results", data=out,
                           file_name="indian_enriched_addresses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
