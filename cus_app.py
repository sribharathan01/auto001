import streamlit as st
import pandas as pd
from datetime import datetime
import io

st.title("Offer List Merger & Filter")

st.markdown("""
Upload the required CSV files below.  
- **Merchant File (EN)**
- **Offer File (AR)**
- **Offer File (EN)**
- **Output Schema File** (previous output, to preserve column order/schema)
""")

# File uploaders
merchant_file = st.file_uploader("Merchant File (EN)", type="csv")
offer_ar_file = st.file_uploader("Offer File (AR)", type="csv")
offer_en_file = st.file_uploader("Offer File (EN)", type="csv")
output_schema_file = st.file_uploader("Output Schema File", type="csv")

if st.button("Process Files"):
    if not (merchant_file and offer_ar_file and offer_en_file and output_schema_file):
        st.error("Please upload all four files.")
    else:
        # Read uploaded files
        merchant_df = pd.read_csv(merchant_file)
        offer_ar_df = pd.read_csv(offer_ar_file)
        offer_df = pd.read_csv(offer_en_file)
        output_df = pd.read_csv(output_schema_file)

        # Rename Arabic columns
        offer_ar_df = offer_ar_df.rename(columns={
            'offer_name': 'offer_name - AR',
            'offer_title': 'offer_title - AR',
            'offer_details': 'offer_details - AR',
            'terms_conditions': 'terms_conditions - AR',
            'how_to_redeem': 'how_to_redeem -AR'
        })

        # Clear output_df but keep schema
        output_df = output_df.iloc[0:0]

        # Merge English and Arabic offer data
        merged_offer_df = pd.merge(
            offer_df,
            offer_ar_df,
            on='cdf_offer_id',
            how='left',
            suffixes=('', '_ar')
        )

        # Get merchant category data
        merchant_categories = merchant_df[['cdf_merchant_id', 'category']]
        # Merge offers with merchant categories
        final_data = pd.merge(
            merged_offer_df,
            merchant_categories,
            on='cdf_merchant_id',
            how='left'
        )

        # Convert 'valid_to' to datetime
        final_data['valid_to'] = pd.to_datetime(final_data['valid_to'], format='%d-%m-%Y', errors='coerce')
        today = pd.to_datetime(datetime.today().date())

        # Filter only active (non-expired) offers
        final_data = final_data[final_data['valid_to'] >= today]

        # Select only the columns that exist in output schema
        for column in output_df.columns:
            if column in final_data.columns:
                output_df[column] = final_data[column].values
            elif column == 'category':
                output_df[column] = final_data['category'].values
            else:
                output_df[column] = None

        # Add 'Status' column
        output_df['Status'] = 'Live on Webapp'

        # Prepare output for download
        output = io.BytesIO()
        output_df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)

        st.success(f"Output file successfully updated with merged data. Total records processed: {len(output_df)}")

        st.download_button(
            label="Download Output CSV",
            data=output,
            file_name="output_file.csv",
            mime="text/csv"
        )

        st.dataframe(output_df.head(10))

