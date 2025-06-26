import streamlit as st
import pandas as pd
from PIL import Image, UnidentifiedImageError
import requests
from io import BytesIO
import re

headers = {'User-Agent': 'Mozilla/5.0'}

def is_valid_url(url):
    return isinstance(url, str) and re.match(r'^https?://', url.strip())

def get_image_resolution(url):
    try:
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        img = Image.open(BytesIO(response.content))
        return img.width, img.height, "Success"
    except (requests.RequestException, UnidentifiedImageError) as e:
        return None, None, str(e)

st.title('Image Resolution Checker')

uploaded_file = st.file_uploader('Upload Excel or CSV file with image URLs (URLs in last column)', type=['xlsx', 'xls', 'csv'])

if uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        url_column = df.columns[-1]
        st.write(f"📌 Using column '{url_column}' for image URLs.")

        widths, heights, messages = [], [], []

        # Progress UI
        progress_text = st.empty()
        progress_bar = st.progress(0)
        total_urls = len(df[url_column])

        for i, url in enumerate(df[url_column]):
            url = str(url).strip()
            if not is_valid_url(url):
                widths.append(None)
                heights.append(None)
                messages.append("Invalid URL")
                progress_text.text(f"❌ Skipped: {url} — Invalid URL ({i+1}/{total_urls})")
                progress_bar.progress(int((i+1)/total_urls*100))
                continue

            width, height, status = get_image_resolution(url)
            widths.append(width)
            heights.append(height)
            messages.append(status)
            progress_text.text(f"✅ Processed: {url} → Width: {width}, Height: {height}, Status: {status} ({i+1}/{total_urls})")
            progress_bar.progress(int((i+1)/total_urls*100))

        # Add columns to original DataFrame
        df['Width'] = widths
        df['Height'] = heights
        df['Status'] = messages

        st.success("✅ Processing complete!")
        st.dataframe(df.head(10))

        # Prepare output for download
        output = BytesIO()
        if uploaded_file.name.endswith('.csv'):
            df.to_csv(output, index=False)
            mime_type = 'text/csv'
            file_name = 'output.csv'
        else:
            df.to_excel(output, index=False)
            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            file_name = 'output.xlsx'
        output.seek(0)

        st.download_button(
            label="Download Output File",
            data=output,
            file_name=file_name,
            mime=mime_type
        )

    except Exception as e:
        st.error(f"Error processing file: {e}")
else:
    st.info("Please upload an Excel or CSV file to start processing.")
