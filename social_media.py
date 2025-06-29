import streamlit as st
import pandas as pd
import io

st.title('Offer Data Processing App')

# --- File uploaders ---
schema_file = st.file_uploader('Upload schema CSV file', type=['csv'])
offer_file = st.file_uploader('Upload offer CSV file', type=['csv'])
offer_ar_file = st.file_uploader('Upload offer AR CSV file', type=['csv'])
merchant_file = st.file_uploader('Upload merchant CSV file', type=['csv'])
location_file = st.file_uploader('Upload location CSV file', type=['csv'])

# --- Offer IDs input ---
offer_ids_input = st.text_area(
    "Enter Offer IDs to keep (comma or newline separated):",
    placeholder="e.g.\n357507,357473,357506\nor\n357507\n357473\n357506"
)

def parse_offer_ids(input_str):
    ids = [x.strip() for x in input_str.replace(',', '\n').split('\n')]
    return [x for x in ids if x]

# --- Main processing block ---
if all([schema_file, offer_file, offer_ar_file, merchant_file, location_file, offer_ids_input]):
    offer_ids_to_keep = parse_offer_ids(offer_ids_input)
    if not offer_ids_to_keep:
        st.error("⚠️ Please enter at least one valid offer ID.")
        st.stop()

    try:
        # Read input files
        schema_df = pd.read_csv(schema_file, dtype=str, encoding='utf-8')
        offer_df = pd.read_csv(offer_file, dtype=str, encoding='utf-8')
        offer_ar_df = pd.read_csv(offer_ar_file, dtype=str, encoding='utf-8')
        merchant_df = pd.read_csv(merchant_file, dtype=str, encoding='utf-8')
        location_df = pd.read_csv(location_file, dtype=str, encoding='utf-8')

        # --- Rename columns for consistency ---
        offer_file_mapping = {
            'offer_name': 'offer_merchant_name',
            'offer_title':'Offer Title',
            'offer_image': 'Offer image',
            'offer_url': 'Offer redemption url',
            'valid_from': 'offer_valid_from',
            'valid_to': 'offer_valid_to',
            'terms_conditions': 'TnC en',
            'redemption_code': 'Redemption Code'
        }
        offer_ar_file_mapping = {
            'offer_name':'offer_name - AR',
            'offer_title':'offer_title - AR',
            'terms_conditions': 'terms_conditions - AR'
        }
        location_file_mapping = {
            'city': 'location'
        }
        merchant_file_mapping = {
            'category': 'category', 
            'brand_logos': 'Logo',
            'merchant_banner_image': 'Banner',
            'merchant_image': 'Merchant Image'
        }

        offer_df.rename(columns=offer_file_mapping, inplace=True)
        offer_ar_df.rename(columns=offer_ar_file_mapping, inplace=True)
        location_df.rename(columns=location_file_mapping, inplace=True)
        merchant_df.rename(columns=merchant_file_mapping, inplace=True)

        # --- Clear existing data but keep schema ---
        schema_df = schema_df.iloc[0:0]

        # --- Get merchant data ---
        merchant_data = merchant_df[['cdf_merchant_id' ,'Logo','Banner','Merchant Image','category']]

        # --- Get location data ---
        location_data = (
            location_df
            .groupby('cdf_offer_id')['location']
            .apply(lambda x: ', '.join(sorted(set(x.dropna()))))
            .reset_index()
        )

        # --- Merge English and Arabic offer data and location data ---
        final_data = pd.merge(
            pd.merge(
                pd.merge(
                    offer_df,
                    offer_ar_df,
                    on='cdf_offer_id',
                    how='left',
                    suffixes=('', '_ar')
                ),
                location_data,
                on='cdf_offer_id',
                how='left'
            ),
            merchant_data,
            on='cdf_merchant_id',
            how='left'
        )

        # --- Map final_data to schema_df columns ---
        for column in schema_df.columns:
            if column in final_data.columns:
                schema_df[column] = final_data[column]
            else:
                schema_df[column] = None  # For columns not found in source files

        # --- Rename and map 'online' column ---
        if 'online' in schema_df.columns:
            schema_df.rename(columns={'online': 'offer_redemption_channel'}, inplace=True)
            schema_df['offer_redemption_channel'] = schema_df['offer_redemption_channel'].map({
                'TRUE': 'Online',
                'FALSE': 'In store'
            })

        # --- Filter rows ---
        filtered_df = schema_df[schema_df['cdf_offer_id'].isin(offer_ids_to_keep)]

        # --- Show filtered data preview ---
        st.write("### Preview of Filtered Data", filtered_df.head())

        # --- Handle empty results ---
        if filtered_df.empty:
            st.warning("⚠️ No matching offers found for the given IDs.")
        else:
            # --- Remove specified columns before export ---
            columns_to_remove = ['cdf_offer_id', 'cdf_merchant_id']
            filtered_df = filtered_df.drop(columns=columns_to_remove, errors='ignore')

            # --- Export filtered data to Excel in-memory ---
            output = io.BytesIO()
            filtered_df.to_excel(output, index=False)
            output.seek(0)

            st.success("✅ Filtered file is ready!")

            # --- Download button ---
            st.download_button(
                label="Download Filtered Offers Output",
                data=output,
                file_name="Filtered_Offers_Output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"❌ Processing failed: {e}")
        st.exception(e)

elif not offer_ids_input:
    st.info("ℹ️ Please enter offer IDs to filter.")

else:
    st.info("ℹ️ Please upload all required input files.")
