import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from io import BytesIO

# --- CONFIGURATION ---
API_KEY = 'AIzaSyBfDa2M6G1JL5kHImqfs9517i6g_9KXwvc'  # Replace with your actual API Key
VALIDATION_ENDPOINT = f'https://addressvalidation.googleapis.com/v1:validateAddress?key={API_KEY}'
GEOCODE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'
REVERSE_GEOCODE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'

# --- STATIC REFERENCE ---
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
        r = requests.get(GEOCODE_ENDPOINT, params=params)
        d = r.json()
        if d.get("status") == "OK":
            result = d["results"][0]
            loc = result["geometry"]["location"]
            return loc["lat"], loc["lng"], True
    except Exception:
        pass
    return None, None, False


def reverse_geocode(lat, lon):
    try:
        params = {'latlng': f'{lat},{lon}', 'key': API_KEY}
        r = requests.get(REVERSE_GEOCODE_ENDPOINT, params=params)
        d = r.json()
        if d.get("status") == "OK":
            components = d["results"][0]["address_components"]
            city, state, pincode = None, None, None
            for c in components:
                if 'locality' in c['types'] or 'sublocality' in c['types']:
                    city = city or c['long_name']
                if 'administrative_area_level_1' in c['types']:
                    state = c['long_name']
                if 'postal_code' in c['types']:
                    pincode = c['long_name']
            return city, state, pincode
    except Exception:
        pass
    return None, None, None


# --- VALIDATE + CORRECT FUNCTION ---
def validate_and_enrich(row):
    address = str(row.get("Address", "")).strip()
    city = row.get("City", None)
    state = row.get("State", None)
    pin = str(row.get("Postal Code")) if pd.notna(row.get("Postal Code")) else None
    lat = row.get("Latitude")
    lon = row.get("Longitude")

    notes = []
    fallback_used = False

    # Step 1: Use full address to get lat/lon
    if (not lat or not lon):
        lat1, lon1, ok = geocode_address(address)
        if ok:
            lat, lon = lat1, lon1
            fallback_used = True
            notes.append("Lat/Lon from full address")

    # Step 2: Reverse geocode from lat/lon to fill missing or invalid data
    if lat and lon:
        rev_city, rev_state, rev_pin = reverse_geocode(lat, lon)

        if not is_valid_indian_pincode(pin) and is_valid_indian_pincode(rev_pin):
            pin = rev_pin
            notes.append("PIN from lat/lon")

        if city not in VALID_CITIES and rev_city in VALID_CITIES:
            city = rev_city
            notes.append("City from lat/lon")

        if state not in VALID_STATES and rev_state in VALID_STATES:
            state = rev_state
            notes.append("State from lat/lon")

    # Step 3: Try using PIN + state to get lat/lon if still missing
    if (not lat or not lon) and is_valid_indian_pincode(pin) and state:
        fallback_address = f"{pin}, {state}, India"
        lat2, lon2, ok2 = geocode_address(fallback_address)
        if ok2:
            lat, lon = lat2, lon2
            notes.append("Lat/Lon from PIN+State")
            fallback_used = True

    # Step 4: Try using city + state if lat/lon still missing
    if (not lat or not lon) and city and state:
        fallback_address = f"{city}, {state}, India"
        lat3, lon3, ok3 = geocode_address(fallback_address)
        if ok3:
            lat, lon = lat3, lon3
            notes.append("Lat/Lon from City+State")
            fallback_used = True

    return {
        "City": city,
        "State": state,
        "Postal Code": pin,
        "Latitude": lat,
        "Longitude": lon,
        "Used_Fallback": fallback_used,
        "Correction_Notes": ", ".join(notes) if notes else "Original data used",
        "Status": "Success" if lat and lon else "Partial/Failed"
    }


# --- GEOCODING WORKER ---
def process_addresses(df, address_col, max_workers):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(validate_and_enrich, row): idx for idx, row in df.iterrows()}
        progress = st.progress(0)
        completed = 0
        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {"Status": "Failed", "Error": str(e)}
            results[idx] = result
            completed += 1
            progress.progress(completed / len(df))
    return results


# --- STREAMLIT UI ---
st.set_page_config(layout="wide")
st.title("📍 Address Validation & Enrichment (India)")

st.markdown("""
Upload Excel containing addresses (address, city, state, zip code columns).  

This app will:
- ✅ Enrich missing ZIPs, cities, states using lat/lon
- ↔ Fill missing lat/lon using address or city/state/zip
- 🧠 Validate and fix wrong data with reverse geocoding

**Note:** Google Maps API key required. Quotas may apply.
""", unsafe_allow_html=True)

uploaded_file = st.file_uploader("📤 Upload Excel File", type=["xlsx"])
column_input = st.text_input("Address column name", value="Address")
max_workers = st.slider("🔄 Parallel workers", 1, 20, 10)

if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write("✅ Sample uploaded data:", df.head())

    if st.button("🚀 Start Processing"):
        st.info("Processing started...")
        results = process_addresses(df, column_input, max_workers)
        results_df = pd.DataFrame(results)
        final = pd.concat([df.reset_index(drop=True), results_df], axis=1)

        st.success("🎉 Processing complete!")
        st.write(final.head())

        output = BytesIO()
        final.to_excel(output, index=False)
        output.seek(0)
        st.download_button(
            label="📥 Download Results as Excel",
            data=output,
            file_name="enriched_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
