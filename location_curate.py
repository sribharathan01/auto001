import streamlit as st
import pandas as pd
import requests
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# --- India Reference Data ---
VALID_STATES = {
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa",
    "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala",
    "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
    "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Puducherry", "Chandigarh",
    "Jammu and Kashmir", "Ladakh", "Andaman and Nicobar Islands", "Dadra and Nagar Haveli and Daman and Diu"
}
VALID_CITIES = {
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune",
    "Jaipur", "Surat", "Lucknow", "Kanpur", "Nagpur", "Indore", "Bhopal", "Patna", "Thane"
}

def is_valid_indian_pincode(pin):
    return bool(re.match(r'^[1-9][0-9]{5}$', str(pin)))

# --- Google Geocoding ---
def geocode_address_google(address, api_key):
    params = {'address': address + ", India", 'key': api_key}
    try:
        response = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
        data = response.json()
        if data.get("status") == "OK":
            loc = data['results'][0]['geometry']['location']
            return loc['lat'], loc['lng'], True
    except:
        pass
    return None, None, False

def reverse_geocode_google(lat, lon, api_key):
    params = {'latlng': f'{lat},{lon}', 'key': api_key}
    try:
        response = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
        data = response.json()
        if data.get("status") == "OK":
            city, state, pin = None, None, None
            for comp in data['results'][0]['address_components']:
                if 'locality' in comp['types'] or 'sublocality' in comp['types']:
                    city = city or comp['long_name']
                if 'administrative_area_level_1' in comp['types']:
                    state = state or comp['long_name']
                if 'postal_code' in comp['types']:
                    pin = pin or comp['long_name']
            return city, state, pin
    except:
        pass
    return None, None, None

# --- Mapbox Geocoding ---
def geocode_address_mapbox(address, api_key):
    import urllib.parse
    query = urllib.parse.quote(address + ", India")
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"
    params = {'access_token': api_key}
    try:
        response = requests.get(url, params=params)
        data = response.json()
        if data.get('features'):
            coords = data['features'][0]['geometry']['coordinates']  # [lon, lat]
            return coords[1], coords[0], True
    except:
        pass
    return None, None, False

def reverse_geocode_mapbox(lat, lon, api_key):
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
    params = {'access_token': api_key}
    try:
        response = requests.get(url, params=params)
        data = response.json()
        city, state, pin = None, None, None
        if data.get('features'):
            for feat in data['features']:
                types = feat.get('place_type', [])
                if 'place' in types and not city:
                    city = feat['text']
                if 'region' in types and not state:
                    state = feat['text']
                if 'postcode' in types and not pin:
                    pin = feat['text']
        return city, state, pin
    except:
        pass
    return None, None, None

# --- Row Processor ---
def validate_and_enrich(row, address_column, provider, api_key):
    address = row.get(address_column, "") or ""
    city = row.get("City", "") or ""
    state = row.get("State", "") or ""
    pin = str(row.get("Postal Code", "") or "")
    lat = row.get("Latitude", None)
    lon = row.get("Longitude", None)

    notes = []
    used_fallback = False

    # Step 1: Geocode if lat/lon missing
    if not lat or not lon:
        if provider == "Google Maps":
            lat1, lon1, success = geocode_address_google(address, api_key)
        else:
            lat1, lon1, success = geocode_address_mapbox(address, api_key)
        if success:
            lat, lon = lat1, lon1
            notes.append("Lat/Lon from geocoding")
            used_fallback = True

    # Step 2: Reverse geocode if city/state/pincode is missing or invalid
    city_valid = city in VALID_CITIES
    state_valid = state in VALID_STATES
    pin_valid = is_valid_indian_pincode(pin)

    if lat and lon and (not city_valid or not state_valid or not pin_valid):
        if provider == "Google Maps":
            rev_city, rev_state, rev_pin = reverse_geocode_google(lat, lon, api_key)
        else:
            rev_city, rev_state, rev_pin = reverse_geocode_mapbox(lat, lon, api_key)

        if not city_valid and rev_city in VALID_CITIES:
            city = rev_city
            notes.append("City updated via reverse geocode")
        if not state_valid and rev_state in VALID_STATES:
            state = rev_state
            notes.append("State updated via reverse geocode")
        if not pin_valid and is_valid_indian_pincode(rev_pin):
            pin = rev_pin
            notes.append("PIN updated via reverse geocode")

    return {
        "Latitude": lat,
        "Longitude": lon,
        "Postal Code": pin,
        "City": city,
        "State": state,
        "Used_Fallback": used_fallback,
        "Correction_Notes": ", ".join(notes) if notes else "No changes",
        "Status": "Success" if lat and lon else "Partial/Failed"
    }

# --- Parallel Batch Processor ---
def process_addresses(df, address_column, provider, api_key, max_threads):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(validate_and_enrich, row, address_column, provider, api_key): idx
            for idx, row in df.iterrows()
        }
        progress = st.progress(0)
        for counter, future in enumerate(as_completed(futures)):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {
                    "Status": "Failed",
                    "Error": str(e)
                }
            progress.progress((counter + 1) / len(df))
    return pd.DataFrame(results)

# --- Streamlit UI ---
st.markdown("""
Upload an Excel file containing Indian addresses.

- Select your geocoding provider (Google Maps or Mapbox)
- Enter your API key or token
- Select the address column
- The tool will enrich and validate Indian addresses using Indian city/state/PIN logic
""")

provider = st.selectbox("Select Geocoding Provider", ["Google Maps", "Mapbox"])
api_key = st.text_input(
    f"Enter your {provider} API Key/Token", value="", type="password"
)
if not api_key:
    st.warning("Please enter a valid API key/token to continue.")
    st.stop()

uploaded_file = st.file_uploader("📤 Upload Excel file", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.success("File uploaded successfully ✅")
    st.dataframe(df.head())

    address_column = st.selectbox("📌 Select the address column:", df.columns)
    threads = st.slider("⚙️ Set number of parallel workers", 2, 20, 10)

    if st.button("🚀 Start Address Validation"):
        st.info("Processing, please wait ⌛...")
        results_df = process_addresses(df, address_column, provider, api_key, threads)
        final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)
        st.success("🎉 Done!")
        st.dataframe(final_df.head())

        excel_output = BytesIO()
        final_df.to_excel(excel_output, index=False)
        excel_output.seek(0)

        st.download_button(
            "⬇️ Download Results",
            data=excel_output,
            file_name="validated_indian_addresses.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
