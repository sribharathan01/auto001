import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from io import BytesIO

# --- CONFIGURATION ---
API_KEY = 'AIzaSyBfDa2M6G1JL5kHImqfs9517i6g_9KXwvc'  # Replace with your actual Google API key
VALIDATION_ENDPOINT = f'https://addressvalidation.googleapis.com/v1:validateAddress?key={API_KEY}'
GEOCODE_ENDPOINT = f'https://maps.googleapis.com/maps/api/geocode/json'
REVERSE_GEOCODE_ENDPOINT = f'https://maps.googleapis.com/maps/api/geocode/json'

# --- STATIC REFERENCE (can be made dynamic) ---
VALID_STATES = {
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa",
    "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala",
    "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
    "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Puducherry", "Chandigarh",
    "Jammu and Kashmir", "Ladakh", "Andaman and Nicobar Islands", "Dadra and Nagar Haveli and Daman and Diu"
}

# Load a list of major Indian cities (you can expand this list)
VALID_CITIES = {"Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune", "Jaipur"}

# --- HELPERS ---
def is_valid_indian_pincode(pin):
    return bool(re.match(r'^[1-9][0-9]{5}$', str(pin)))

def geocode_address(address):
    params = {'address': address, 'key': API_KEY}
    try:
        response = requests.get(GEOCODE_ENDPOINT, params=params)
        data = response.json()
        if data.get('status') == 'OK':
            location = data['results'][0]['geometry']['location']
            return location.get('lat'), location.get('lng'), True
    except Exception:
        pass
    return None, None, False

def reverse_geocode_zipcode(lat, lon):
    params = {'latlng': f'{lat},{lon}', 'key': API_KEY}
    try:
        response = requests.get(REVERSE_GEOCODE_ENDPOINT, params=params)
        data = response.json()
        if data.get('status') == 'OK':
            for component in data['results'][0]['address_components']:
                if 'postal_code' in component['types']:
                    return component['long_name']
    except Exception:
        pass
    return None

def reverse_geocode_city_state(lat, lon):
    params = {'latlng': f'{lat},{lon}', 'key': API_KEY}
    try:
        response = requests.get(REVERSE_GEOCODE_ENDPOINT, params=params)
        data = response.json()
        if data.get('status') == 'OK':
            city = None
            state = None
            for component in data['results'][0]['address_components']:
                if 'locality' in component['types'] or 'sublocality' in component['types']:
                    city = component['long_name']
                if 'administrative_area_level_1' in component['types']:
                    state = component['long_name']
            return city, state
    except Exception:
        pass
    return None, None

def validate_address(address, retries=3):
    payload = {
        "address": {
            "regionCode": "IN",
            "addressLines": [address]
        }
    }

    for attempt in range(retries):
        try:
            response = requests.post(VALIDATION_ENDPOINT, json=payload)
            data = response.json()

            if response.status_code == 429:
                time.sleep(5)
                continue
            elif response.status_code != 200:
                return {'Status': 'Failed', 'Error': f"HTTP {response.status_code}"}

            addr_info = data.get('result', {}).get('address', {})
            location = addr_info.get('location', {})
            postal = addr_info.get('postalAddress', {})

            lat = location.get('latitude')
            lon = location.get('longitude')
            city = postal.get('locality')
            state = postal.get('administrativeArea')
            zipcode = postal.get('postalCode')

            used_fallback = False
            status_notes = []

            # --- Fallback to geocoding if lat/lon missing ---
            if lat is None or lon is None:
                lat, lon, used_fallback = geocode_address(address)
                if used_fallback:
                    status_notes.append("Lat/Lon via fallback")

            # --- Validate ZIP ---
            if not is_valid_indian_pincode(zipcode) and lat and lon:
                zip_rev = reverse_geocode_zipcode(lat, lon)
                if is_valid_indian_pincode(zip_rev):
                    zipcode = zip_rev
                    status_notes.append("Zip corrected from lat/lon")
                else:
                    status_notes.append("Invalid zip")

            # --- Validate city/state ---
            city_valid = city in VALID_CITIES
            state_valid = state in VALID_STATES
            city_state_corrected = False

            if lat and lon and (not city_valid or not state_valid):
                city_rev, state_rev = reverse_geocode_city_state(lat, lon)
                if city_rev in VALID_CITIES:
                    city = city_rev
                    city_state_corrected = True
                if state_rev in VALID_STATES:
                    state = state_rev
                    city_state_corrected = True
                if city_state_corrected:
                    status_notes.append("City/State corrected via coord")

            # --- Final status ---
            status = "Success"
            if status_notes:
                status += " (" + ", ".join(status_notes) + ")"

            return {
                'City': city,
                'State': state,
                'Postal Code': zipcode,
                'Latitude': lat,
                'Longitude': lon,
                'Used_Fallback_Geocoding': used_fallback,
                'City/State_Corrected': city_state_corrected,
                'Status': status
            }

        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                return {
                    'City': None,
                    'State': None,
                    'Postal Code': None,
                    'Latitude': None,
                    'Longitude': None,
                    'Used_Fallback_Geocoding': False,
                    'City/State_Corrected': False,
                    'Status': 'Failed',
                    'Error': str(e)
                }

def process_row(row, address_column):
    address = str(row[address_column])
    return validate_address(address)

def process_addresses(df, address_column, max_workers=10):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_row, row, address_column): idx for idx, row in df.iterrows()}
        progress = st.progress(0)
        completed = 0
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {'Status': 'Failed', 'Error': str(exc)}
            results[idx] = result
            completed += 1
            progress.progress(completed / len(df))
    return results

# --- STREAMLIT UI ---
st.title("🗺 Bulk Address Validation & Geocoding (India)")

st.markdown(
    """
    Upload an Excel file containing addresses.<br>
    The script will validate, geocode, and correct city/state info using Google Maps API.<br>
    <b>Note:</b> Your Google API key quota and rate limits apply.
    """,
    unsafe_allow_html=True,
)

uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])
address_column = st.text_input("Column name containing addresses", value="Address")
max_workers = st.slider("Parallel workers", min_value=1, max_value=20, value=10)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write("Preview of uploaded data:", df.head())

    if st.button("Start Processing"):
        st.info("Processing addresses. Please wait...")
        results = process_addresses(df, address_column, max_workers=max_workers)
        results_df = pd.DataFrame(results)
        final_df = pd.concat([df, results_df], axis=1)
        st.success("✅ Processing complete!")

        # Show a preview
        st.write(final_df.head())

        # Download link
        output = BytesIO()
        final_df.to_excel(output, index=False)
        output.seek(0)
        st.download_button(
            label="📥 Download Results as Excel",
            data=output,
            file_name="validated_addresses_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
