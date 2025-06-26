import pandas as pd
from io import StringIO

def process_files(schema_file_content, offer_file_content, offer_ar_file_content, merchant_file_content, location_file_content, offer_ids_to_keep):
    # Read input files from uploaded content
    schema_df = pd.read_csv(StringIO(schema_file_content), dtype=str, encoding='utf-8')
    offer_df = pd.read_csv(StringIO(offer_file_content), dtype=str, encoding='utf-8')
    offer_ar_df = pd.read_csv(StringIO(offer_ar_file_content), dtype=str, encoding='utf-8')
    merchant_df = pd.read_csv(StringIO(merchant_file_content), dtype=str, encoding='utf-8')
    location_df = pd.read_csv(StringIO(location_file_content), dtype=str, encoding='utf-8')

    # Rename columns for consistency
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

    # Clear existing data but keep schema
    schema_df = schema_df.iloc[0:0]

    # Get merchant data
    merchant_data = merchant_df[['cdf_merchant_id' ,'Logo','Banner','Merchant Image','category']]

    # Get location data
    location_data = (
        location_df
        .groupby('cdf_offer_id')['location']
        .apply(lambda x: ', '.join(sorted(set(x.dropna()))))
        .reset_index()
    )

    # Merge English and Arabic offer data and location data
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

    for column in schema_df.columns:
        if column in final_data.columns:
            schema_df[column] = final_data[column]
        else:
            schema_df[column] = None  # For columns not found in source files

    # Rename and map 'online' column
    if 'online' in schema_df.columns:
        schema_df.rename(columns={'online': 'offer_redemption_channel'}, inplace=True)
        schema_df['offer_redemption_channel'] = schema_df['offer_redemption_channel'].map({
            'TRUE': 'Online',
            'FALSE': 'In store'
        })

    # Filter rows by offer IDs
    filtered_df = schema_df[schema_df['cdf_offer_id'].isin(offer_ids_to_keep)]

    # Return filtered dataframe
    return filtered_df
