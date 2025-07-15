import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from io import BytesIO

# --- CONFIGURATION ---
API_KEY = 'AIzaSyBfDa2M6G1JL5kHImqfs9517i6g_9KXwvc'  # 🎯 Replace this with your actual Google Maps API key
GEOCODE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'
REVERSE_GEOCODE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'


# --- HELPERS ---
def geocode_address_from_fields(address, city, state, postal_code, country):
    """
    Try to geocode using whatever address components are available.
    """
    components = [address, city, state, postal_code, country]
    query = ', '.join([str(part).strip() for part in components if part])
    params = {'address': query, 'key': API_KEY}

    try:
        res = requests.get(GEOCODE_ENDPOINT, params=params)
        data = res.json()
        if data.get('status') == 'OK' and data['results']:
            loc = data['results'][0]['geometry']['location']
            return loc.get('lat'), loc.get('lng'), True
    except Exception:
        pass
    return None, None, False


def reverse_geocode(lat, lon):
    """Convert latitude and longitude to address components."""
    try:
        params = {'latlng': f'{lat},{lon}', 'key': API_KEY}
        res = requests.get(REVERSE_GEOCODE_ENDPOINT, params=params)
        data = res.json()
        if data.get('status') == 'OK' and data['results']:
            components = data['results'][0]['address_components']
            city, state, postal, country = None, None, None, None
            for comp in components:
                if 'locality' in comp['types'] or 'sublocality' in comp['types']:
                    city = city or comp['long_name']
                if 'administrative_area_level_1' in comp['types']:
                    state = comp['long_name']
                if 'postal_code' in comp['types']:
                    postal = comp['long_name']
                if 'country' in comp['types']:
                    country = comp['long_name']
            return city, state, postal, country
    except Exception:
        pass
    return None, None, None, None


# --- MAIN VALIDATION FUNCTION ---
def validate_and_enrich(row):
    address = row.get("Address", "") or ""
    city = row.get("City", "") or ""
    state = row.get("State", "") or ""
    postal = row.get("Postal Code", "") or ""
    country = row.get("Country", "") or ""
    lat = row.get("Latitude")
    lon = row.get("Longitude")

    notes = []
    used_fallback = False

    # Step 1: If lat/lon is missing, try to geocode full address
    if not lat or not lon:
        lat1, lon1, success = geocode_address_from_fields(address, city, state, postal, country)
        if success:
            lat, lon = lat1, lon1
            used_fallback = True
            notes.append("Lat/Lon from geocoding all fields")

    # Step 2: If lat/lon exists but other fields missing, try reverse geocoding
    if lat and lon:
        r_city, r_state, r_postal, r_country = reverse_geocode(lat, lon)

        if not city and r_city:
            city = r_city
            notes.append("City from reverse geocoding")

        if not state and r_state:
            state = r_state
            notes.append("State from reverse geocoding")

        if not postal and r_postal:
            postal = r_postal
            notes.append("Postal Code from reverse geocoding")

        if not country and r_country:
            country = r_country
            notes.append("Country from reverse geocoding")

    return {
        "Latitude": lat,
        "Longitude": lon,
        "City": city,
        "State": state,
        "Postal Code": postal,
        "Country": country,
        "Used_Fallback": used_fallback,
        "Correction_Notes": ", ".join(notes) if notes else "No correction needed",
        "Status": "Success" if lat and lon else "Partial/Failed"
    }


# --- MULTI-THREAD WORKER ---
def process_addresses(df, address_col, max_workers):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(validate_and_enrich, row): idx for idx, row in df.iterrows()}
        progress = st.progress(0)
        for count, future in enumerate(as_completed(futures)):
            idx = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"Status": "Failed", "Error": str(e)}
            results[idx] = result
            progress.progress((count + 1) / len(df))
    return pd.DataFrame(results)


# --- STREAMLIT UI ---
st.set_page_config(layout="wide")
st.title("🌍 Global Address Validator & Enricher")

st.markdown("""
Upload an Excel file with addresses to enrich missing values like:
- 📌 Latitude / Longitude
- 🏙 City, State
- 🌎 Country and Postal Code

Powered by Google Maps APIs 🌐  
Note: You **must enter your API key** in the script for this to work.

""", unsafe_allow_html=True)

uploaded_file = st.file_uploader("📂 Upload Excel file", type=["xlsx"])
address_col = st.text_input("📬 Column name for full address:", value="Address")
max_workers = st.slider("🚀 Parallel threads", 2, 20, 10)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write("📄 File loaded. Preview below:")
    st.dataframe(df.head())

    if st.button("▶️ Start Validation"):
        st.info("Working... Please wait.")
        result_df = process_addresses(df, address_col, max_workers)
        final_df = pd.concat([df.reset_index(drop=True), result_df], axis=1)
        st.success("✅ Done!")
        st.dataframe(final_df.head())

        output = BytesIO()
        final_df.to_excel(output, index=False)
        output.seek(0)

        st.download_button("⬇️ Download Result", data=output, file_name="validated_addresses.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
