import streamlit as st
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

# --- PROVIDER SELECTION UI ---
st.set_page_config(layout="wide")
st.title("🌎 Global Address Validator & Enricher")

provider = st.selectbox("Select Geocoding Provider", ["Google Maps", "Mapbox"])

if provider == "Google Maps":
    api_key = st.text_input("AIzaSyBfDa2M6G1JL5kHImqfs9517i6g_9KXwvc", value="", type="password")
else:
    api_key = st.text_input("pk.eyJ1Ijoic3JpYmhhcmF0aGFuIiwiYSI6ImNtY2xudXZmeTBhMXUycXNkMml0Z2F1YjAifQ.QI6GXw21q7uWZAotQSrW6w", value="", type="password")

if not api_key:
    st.warning("Please enter a valid API key or token to continue.")
    st.stop()

# --- Google Geocoding ---
def geocode_address_google(address, city, state, postal_code, country, api_key):
    components = [address, city, state, postal_code, country]
    query = ', '.join([str(part).strip() for part in components if part])
    params = {'address': query, 'key': api_key}
    try:
        res = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
        data = res.json()
        if data.get('status') == 'OK' and data['results']:
            loc = data['results'][0]['geometry']['location']
            return loc['lat'], loc['lng'], True
    except Exception as e:
        pass
    return None, None, False

# --- Mapbox Geocoding ---
def geocode_address_mapbox(address, city, state, postal_code, country, api_key):
    components = [address, city, state, postal_code, country]
    query = ', '.join([str(part).strip() for part in components if part])
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{requests.utils.quote(query)}.json"
    params = {'access_token': api_key}
    try:
        res = requests.get(url, params=params)
        data = res.json()
        if data.get('features'):
            coords = data['features'][0]['geometry']['coordinates']  # [lon, lat]
            return coords[1], coords[0], True
    except Exception as e:
        pass
    return None, None, False

# --- Google Reverse Geocoding ---
def reverse_geocode_google(lat, lon, api_key):
    params = {'latlng': f'{lat},{lon}', 'key': api_key}
    try:
        res = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
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
    except Exception as e:
        pass
    return None, None, None, None

# --- Mapbox Reverse Geocoding ---
def reverse_geocode_mapbox(lat, lon, api_key):
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
    params = {'access_token': api_key}
    try:
        res = requests.get(url, params=params)
        data = res.json()
        city, state, postal, country = None, None, None, None
        if data.get('features'):
            for feat in data['features']:
                types = feat.get('place_type', [])
                if 'place' in types and not city:
                    city = feat['text']
                if 'region' in types and not state:
                    state = feat['text']
                if 'postcode' in types and not postal:
                    postal = feat['text']
                if 'country' in types and not country:
                    country = feat['text']
        return city, state, postal, country
    except Exception as e:
        pass
    return None, None, None, None

# --- Main Enrichment Function ---
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

    # Step 1: Geocoding
    if not lat or not lon:
        if provider == "Google Maps":
            lat1, lon1, success = geocode_address_google(address, city, state, postal, country, api_key)
        else:
            lat1, lon1, success = geocode_address_mapbox(address, city, state, postal, country, api_key)
        if success:
            lat, lon = lat1, lon1
            used_fallback = True
            notes.append("Lat/Lon from geocoding")

    # Step 2: Reverse Geocoding
    if lat and lon:
        if provider == "Google Maps":
            r_city, r_state, r_postal, r_country = reverse_geocode_google(lat, lon, api_key)
        else:
            r_city, r_state, r_postal, r_country = reverse_geocode_mapbox(lat, lon, api_key)

        if not city and r_city:
            city = r_city
            notes.append("City from reverse geocoding")
        if not state and r_state:
            state = r_state
            notes.append("State from reverse geocoding")
        if not postal and r_postal:
            postal = r_postal
            notes.append("Postal code from reverse geocoding")
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

# --- Threaded Address Processor ---
def process_addresses(df, max_workers):
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

# --- Streamlit UI: File Upload & Process ---
st.markdown("Upload an Excel file to enrich missing address values using selected provider.")
uploaded_file = st.file_uploader("📂 Upload Excel File", type=["xlsx"])
max_workers = st.slider("🧵 Max Parallel Threads", 2, 20, 10)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.info("✅ File Loaded. Preview below 👇")
    st.dataframe(df.head())

    if st.button("🚀 Start Address Enrichment"):
        st.info("Please wait, processing...")
        result_df = process_addresses(df, max_workers)
        final_df = pd.concat([df.reset_index(drop=True), result_df], axis=1)
        st.success("✅ Enrichment Complete!")
        st.write(final_df.head())

        output = BytesIO()
        final_df.to_excel(output, index=False)
        output.seek(0)
        st.download_button(
            label="⬇️ Download Results",
            data=output,
            file_name="enriched_addresses.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
