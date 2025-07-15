import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from io import BytesIO

# --- CONFIGURATION ---
API_KEY = 'YOUR_API_KEY_HERE'  # Replace with your valid Google API key
VALIDATION_ENDPOINT = f'https://addressvalidation.googleapis.com/v1:validateAddress?key={API_KEY}'
GEOCODE_ENDPOINT = f'https://maps.googleapis.com/maps/api/geocode/json'
REVERSE_GEOCODE_ENDPOINT = f'https://maps.googleapis.com/maps/api/geocode/json'

# --- STATIC VALID REFERENCE SETS ---
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
    "Jaipur", "Surat", "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal", "Patna"
}

# --- HELPERS ---
def is_valid_indian_pincode(pin):
    return bool(re.match(r'^[1-9][0-9]{5}$', str(pin)))

def geocode_address(address):
    params = {'address': address, 'key': API_KEY}
    try:
        response = requests.get(GEOCODE_ENDPOINT, params=params)
        data = response.json()
        if data.get('status') == 'OK' and data['results']:
            location = data['results'][0]['geometry']['location']
            return location.get('lat'), location.get('lng'), True
    except Exception:
        pass
    return None, None, False

def reverse_geocode(lat, lon):
    params = {'latlng': f'{lat},{lon}', 'key': API_KEY}
    try:
        response = requests.get(REVERSE_GEOCODE_ENDPOINT, params=params)
        result = response.json()
        if result.get('status') == 'OK':
            components = result['results'][0]['address_components']
            city, state, postal = None, None, None
            for comp in components:
                if 'locality' in comp['types'] or 'sublocality' in comp['types']:
                    city = city or comp['long_name']
                if 'administrative_area_level_1' in comp['types']:
                    state = state or comp['long_name']
                if 'postal_code' in comp['types']:
                    postal = comp['long_name']
            return city, state, postal
    except Exception:
        pass
    return None, None, None

def geocode_from_zip_state(zipcode, state):
    if not is_valid_indian_pincode(zipcode) or not state:
        return None, None, False
    address = f"{zipcode}, {state}, India"
    return geocode_address(address)

# --- VALIDATION + ENRICHMENT LOGIC ---
def validate_and_enrich(row):
    address = str(row.get('Address', ''))
    city = row.get('City', None)
    state = row.get('State', None)
    pin = str(row.get('Postal Code')) if pd.notna(row.get('Postal Code')) else None
    lat = row.get('Latitude', None)
    lon = row.get('Longitude', None)

    status = []
    used_fallback = False

    # 1. Use original address to fetch everything
    lat1, lon1, direct_geocode = geocode_address(address)

    if not lat and lat1:
        lat, lon = lat1, lon1
        status.append("Lat/Lon from address")
        used_fallback = True

    # 2. Reverse Geocode from lat/lon if city/state/pin not valid
    need_city = city not in VALID_CITIES
    need_state = state not in VALID_STATES
    need_pin = not is_valid_indian_pincode(pin)

    if lat and lon and (need_city or need_state or need_pin):
        rev_city, rev_state, rev_pin = reverse_geocode(lat, lon)

        if need_city and rev_city in VALID_CITIES:
            city = rev_city
            status.append("City from coord")

        if need_state and rev_state in VALID_STATES:
            state = rev_state
            status.append("State from coord")

        if need_pin and is_valid_indian_pincode(rev_pin):
            pin = rev_pin
            status.append("Zip from coord")

    # 3. If zip/state available, try getting lat/lon
    if (not lat or not lon) and is_valid_indian_pincode(pin) and state:
        lat2, lon2, success = geocode_from_zip_state(pin, state)
        if success and lat2 and lon2:
            lat, lon = lat2, lon2
            used_fallback = True
            status.append("Lat/Lon from zip+state")

    return {
        "Latitude": lat,
        "Longitude": lon,
        "City": city,
        "State": state,
        "Postal Code": pin,
        "Used_Fallback_Geocoding": used_fallback,
        "Status": "Success" if lat and lon else "Partial/Failed" + (" (" + ", ".join(status) + ")" if status else "")
    }

# --- CONCURRENT PROCESSING ---
def process_addresses(df, address_column, max_workers=10):
    results = [None] * len(df)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, row in df.iterrows():
            row_data = dict(row)
            row_data['Address'] = str(row[address_column])
            futures[executor.submit(validate_and_enrich, row_data)] = idx

        progress = st.progress(0)
        completed = 0

        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {'Status': 'Failed', 'Error': repr(e)}
            results[idx] = result
            completed += 1
            progress.progress(completed / len(df))
    return results

# --- STREAMLIT UI ---
st.title("📍 Address Enrichment & Validation (India)")

st.markdown(
    """
Upload an Excel file with address data.  
This app will:
- 🧠 Geocode using address/postal/city/state/lat/lon
- 🔁 Cross-fill missing values from available fields
- ✅ Validate & correct wrong city/state/zip using coordinates
""",
    unsafe_allow_html=True,
)

uploaded_file = st.file_uploader("📤 Upload Excel file", type=["xlsx"])
address_column = st.text_input("📌 Column name of address", value="Address")
max_workers = st.slider("🔄 Parallel workers", 1, 20, 10)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write("📋 Preview of uploaded data:")
    st.dataframe(df.head())

    if st.button("▶️ Start Processing"):
        st.info("⏳ Processing records...")
        results = process_addresses(df, address_column, max_workers=max_workers)
        results_df = pd.DataFrame(results)
        final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

        st.success("✅ Processing completed!")
        st.dataframe(final_df.head())

        output = BytesIO()
        final_df.to_excel(output, index=False)
        output.seek(0)

        st.download_button(
            label="📥 Download Complete File",
            data=output,
            file_name="enriched_addresses.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
