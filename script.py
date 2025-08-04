fceda_data_prep
import json
import pandas as pd
import boto3
import io
import os

#Initial Cleaning includes:
    #drop duplicates
    #keep reuqired columns
    #keep active businessess
    #keep businesses in VA only
    #transform IncorpDate to date format (m-d-y)
    #add business type for classification

# Define cleaning function for 'Officer.csv'
def clean_officer_file(df):
    df = df.apply(lambda x: x.str.replace(r'\t', '', regex=True) if x.dtype == "object" else x)
    keep_columns = ['EntityID', 'OfficerLastName', 'OfficerFirstName', 'OfficerTitle']
    df = df[keep_columns]
    df = df.apply(lambda col: col.str.title().str.strip() if col.dtype == "object" else col)
    df = df.fillna('x')
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df = df.applymap(lambda x: 'x' if isinstance(x, str) and x.strip() == '' else x)
    return df

def clean_lp_file(df):
    df = df.apply(lambda x: x.str.replace('\t', '', regex=False) if x.dtype == "object" else x)
    keep_columns = ['EntityID','Name', 'Status', 'IncorpDate', 'IncorpState', 'Street1', 'Street2', 'City', 'State', 'Zip']
    df = df[keep_columns]
    df['Status'] = df['Status'].str.strip()
    df['State'] = df['State'].str.strip()
    df = df[(df['Status'] == 'ACTIVE') & (df['State'] == 'Virginia')]
    df = df.drop_duplicates()
    df = df.applymap(lambda x: x.title() if isinstance(x, str) else x)
    df['IncorpDate'] = pd.to_datetime(df['IncorpDate']).dt.strftime('%m-%d-%Y')
    df['Zip'] = df['Zip'].str.slice(0, 5)
    df['BusinessType'] = 'LP'
    return df

def clean_llc_file(df):
    df = df.apply(lambda x: x.str.replace('\t', '', regex=False) if x.dtype == "object" else x)
    keep_columns = ['EntityID','Name', 'Status', 'IncorpDate', 'IncorpState', 'Street1', 'Street2', 'City', 'State', 'Zip']
    df = df[keep_columns]
    df['Status'] = df['Status'].str.strip()
    df['State'] = df['State'].str.strip()
    df = df[(df['Status'] == 'ACTIVE') & (df['State'] == 'Virginia')]
    df = df.drop_duplicates()
    df = df.applymap(lambda x: x.title() if isinstance(x, str) else x)
    df['IncorpDate'] = pd.to_datetime(df['IncorpDate'], errors='coerce').dt.strftime('%m-%d-%Y')
    df['Zip'] = df['Zip'].str.slice(0, 5)
    df['BusinessType'] = 'LLC'
    return df

def clean_corp_file(df):
    df = df.apply(lambda x: x.str.replace('\t', '', regex=False) if x.dtype == "object" else x)
    keep_columns = ['EntityID','Name', 'Status', 'IncorpDate', 'IncorpState', 'Street1', 'Street2', 'City', 'State', 'Zip']
    df = df[keep_columns]
    df['Status'] = df['Status'].str.strip()
    df['State'] = df['State'].str.strip()
    df = df[(df['Status'] == 'ACTIVE') & (df['State'] == 'Virginia')]
    df = df.drop_duplicates()
    df = df.applymap(lambda x: x.title() if isinstance(x, str) else x)
    df['IncorpDate'] = pd.to_datetime(df['IncorpDate'], errors='coerce').dt.strftime('%m-%d-%Y')
    df['Zip'] = df['Zip'].str.slice(0, 5)
    df['BusinessType'] = 'Corp'
    return df

def clean_gp_file(df):
    df = df.apply(lambda x: x.str.replace('\t', '', regex=False) if x.dtype == "object" else x)
    keep_columns = ['EntityID','Name', 'Status', 'IncorpDate', 'IncorpState', 'Street1', 'Street2', 'City', 'State', 'Zip']
    df = df[keep_columns]
    df['Status'] = df['Status'].str.strip()
    df['State'] = df['State'].str.strip()
    df = df[(df['Status'] == 'ACTIVE') & (df['State'] == 'Virginia')]
    df = df.drop_duplicates()
    df = df.applymap(lambda x: x.title() if isinstance(x, str) else x)
    df['IncorpDate'] = pd.to_datetime(df['IncorpDate'], errors='coerce').dt.strftime('%m-%d-%Y')
    df['Zip'] = df['Zip'].str.slice(0, 5)
    df['BusinessType'] = 'GP'
    return df

def clean_building_type_file(df):
    df = df.apply(lambda x: x.str.replace(r'\t', '', regex=True) if x.dtype == "object" else x)
    df = df.apply(lambda col: col.str.title().str.strip() if col.dtype == "object" else col)
    df = df.fillna('')
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    keep_columns = [
        'Permit Number/ID:', 'Name:', 'Code/Year:', 'Group(s):', 
        'Address:', 'City:', 'Zip Code:', 'GlobalID', 'XCords', 'YCords'
    ]
    df = df[keep_columns] if all(col in df.columns for col in keep_columns) else df
    if 'Group(s):' in df.columns:
        df['Cleaned Group(s)'] = df['Group(s):'].apply(lambda x: ''.join(filter(str.isalpha, x))[:1] if isinstance(x, str) else None)
    df.columns = df.columns.str.strip()
    return df

cleaning_strategies = {
    'Officer.csv': clean_officer_file,
    'LP.csv': clean_lp_file,
    'LLC.csv': clean_llc_file, 
    'Corp.csv': clean_corp_file, 
    'GP.csv': clean_gp_file,
    'Building_Type_of_Construction.csv': clean_building_type_file
}

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    response = s3.list_objects_v2(Bucket=bucket_name)
    files_to_process = response.get('Contents', [])

    while response.get('IsTruncated'):
        continuation_token = response.get('NextContinuationToken')
        response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
        files_to_process.extend(response.get('Contents', []))

    cleaned_bucket = 'cleaned-fceda-data'
    cleaned_prefix = 'cleaned_data/'
    merged_bucket = 'merged-fceda-data'
    merged_key = 'merged_data/joined_df.csv'
    zip_bucket = 'gmu-fceda-project'
    zip_key = 'ZIP_Codes.csv'

#Join Businesses data together (LP,LLC,Corp,GP)
business_dfs = []

for file in files_to_process:
    file_key = file['Key']
    filename = os.path.basename(file_key)
    print(f" Processing file: {file_key}")

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        df = pd.read_csv(io.BytesIO(file_content), dtype={'EntityID': str}, index_col=False)

        cleaning_function = cleaning_strategies.get(filename)

        if cleaning_function:
            df_cleaned = cleaning_function(df)
            cleaned_data = df_cleaned.to_csv(index=False).encode('utf-8')
            cleaned_key = cleaned_prefix + filename 
            s3.put_object(Bucket=cleaned_bucket, Key=cleaned_key, Body=cleaned_data)
            print(f"Cleaned file uploaded to {cleaned_bucket}/{cleaned_key}")

            if filename in ['LLC.csv', 'LP.csv', 'GP.csv', 'Corp.csv']:
                business_dfs.append(df_cleaned)
        else:
            print(f"WARNING: File '{filename}' was skipped — no cleaning strategy found.")
            print("Possible reasons: typo, unsupported file, or misnamed upload.")

    except Exception as e:
        print(f"Error processing file {file_key}: {e}")
        continue

#Join with FFX County Zipcode File for inital filtering (still contains businesses using shared zipcode with other county & Fairfax City)
    if business_dfs:
        try:
            zip_obj = s3.get_object(Bucket=zip_bucket, Key=zip_key)
            df_zip = pd.read_csv(io.BytesIO(zip_obj['Body'].read()))
            df_zip['ZIPCODE'] = df_zip['ZIPCODE'].astype(str).str.strip()
            df_zip = df_zip[['ZIPCODE', 'Shape__Area', 'Shape__Length']]

            business_df = pd.concat(business_dfs, ignore_index=True)
            business_df['Zip'] = business_df['Zip'].astype(str).str.strip()

            df_joined = pd.merge(business_df, df_zip, left_on='Zip', right_on='ZIPCODE', how='left')
            df_joined = df_joined[df_joined['ZIPCODE'].notna()]
            df_joined.drop(columns=['Zip'], inplace=True)

            merged_csv = df_joined.to_csv(index=False).encode('utf-8-sig')
            s3.put_object(Bucket=merged_bucket, Key=merged_key, Body=merged_csv)
            print(f"Joined file uploaded to {merged_bucket}/{merged_key}")

        except Exception as e:
            print(f"Error during merging: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Files processed and merged successfully!')
    }



fceda-address-standardization
import json
import pandas as pd
import re
import boto3
from io import StringIO
from rapidfuzz import process, fuzz

s3 = boto3.client('s3')

VALID_CITIES = [
    "Alexandria", "Annandale", "Arlington", "Baileys Crossroads", "Burke", "Centreville",
    "Chantilly", "Clifton", "Dunn Loring", "Falls Church", "Fairfax",
    "Fairfax Station", "Fort Belvoir", "Franconia", "Great Falls",
    "Herndon", "Kingstowne", "Leesburg", "Lincolnia", "Lorton", "Manassas", "McLean",
    "Merrifield", "Mount Vernon", "Oak Hill", "Oakton", "Reston",
    "Seven Corners", "South Riding", "Springfield", "Sterling", "Tysons Corner",
    "Vienna", "West McLean", "Woodbridge"
]

    # --- Clean individual row ---
def clean_row(row):
    street1 = str(row.get("Street1", "") or "")
    street2 = str(row.get("Street2", "") or "")
    city = str(row.get("City", "") or "")
    building_name = str(row.get("BuildingName", "") or "")

    # Standardize all inputs
    street1 = street1.strip()
    street2 = street2.strip()
    building_name = building_name.strip()
    city = city.strip()

    street_keywords = r"\b(Rd|St|Blvd|Way|Dr|Hwy|Ave|Ln|Ct|Cir|Pl|Pkwy|Parkway|Plaza|Trail|Route|Rt|Ter|Sq|Square|Park|Highway|Terrace|Trail|Lane|Way|Court|Circle|Pike|Drive|Road|Street|Cluster|Pk|Terr|Boulevard|Avenue|Place)\b"
    suite_keywords = r"\b(Suite\s*\d+\w*|Ste\s*\d+\w*|Apt\s*\d+\w*|Apartment\s*\d+\w*|#\s*\d+\w*|Unit\s*\d+\w*)\b"
    suite_trailing_pattern = r"(#?\s*\d+[A-Za-z]?)$"
    po_box_pattern = r"\b(P\.?\s*O\.?\s*Box\s*\d+)\b"
    care_of_pattern = r"\b(C\/O|C\s*\/\s*O|Care\s*Of|Attention\s*:|Att\s*:|Co\.)\s*([^\d]+?)(?=\d|$)"
    street_type_only = ["Street", "Terrace", "Road", "Dr", "Drive", "Ave", "Avenue", "Ln", "Lane", 
                        "Court", "Circle", "Place", "Boulevard", "Blvd", "Highway", "Hwy", "Pkwy", "Parkway"]

    # --- STEP 1: First, handle cases where entire address is in building_name and street1 is a street type ---
    if building_name and street1 in street_type_only:
        if re.search(r"\d+", building_name):  # Ensure building_name has numbers (likely address)
            # Move building_name to street1 and append street type
            street1 = building_name + street1
            building_name = ""

    # --- STEP 2: Handle when building_name is a number and street1 starts with text ---
    if building_name and building_name.strip().isdigit() and street1 and not street1[0].isdigit():
        street1 = building_name + street1
        building_name = ""

    # --- STEP 3: Fix for C/O records ---
    care_of_match = re.search(care_of_pattern, street1, re.IGNORECASE)
    if care_of_match:
        prefix = care_of_match.group(0)
        remaining = street1[len(prefix):].strip()
        
        # Move C/O to building name
        if not building_name:
            building_name = prefix.strip()
        else:
            building_name = prefix.strip() + " " + building_name.strip()
            
        # Set remaining text as street1
        street1 = remaining
    
    street1, street2 = extract_po_box(street1, street2, po_box_pattern)
    building_name, street2 = extract_po_box(building_name, street2, po_box_pattern, prefer_existing=street2)

    # --- Handle Plaza America Tower type cases ---
    building_name, street1, street2 = handle_plaza_america_cases(building_name, street1, street2)

    # --- Handle case where street address is in building name ---
    if building_name and re.search(r"^\d+", building_name) and (not street1.strip() or street1.strip() in ["Street", "Terrace", "Road", "Drive", "Avenue", "Lane", "Court", "Circle", "Place", "Boulevard", "Highway"]):
        # If building name starts with a number and street1 is empty OR just a street type
        # Move building name to street1 and street1 (if it's a street type) to street2
        if street1.strip() in ["Street", "Terrace", "Road", "Drive", "Avenue", "Lane", "Court", "Circle", "Place", "Boulevard", "Highway"]:
            street2 = street1.strip()
        street1 = building_name
        building_name = ""
    elif not re.search(street_keywords, street1, re.IGNORECASE) and re.search(street_keywords, building_name, re.IGNORECASE):
        temp = building_name
        building_name = street1 if street1.strip() else ""
        street1 = temp

    # --- Handle case where street1 has no street keywords but street2 does ---
    if not re.search(street_keywords, street1, re.IGNORECASE) and re.search(street_keywords, street2, re.IGNORECASE):
        # Special case for "1005 North Sycamore" in building name and "Street" in street1
        if street1.strip() in ["Street", "Terrace", "Road", "Drive", "Avenue", "Lane", "Court", "Circle", "Place", "Boulevard", "Highway"]:
            street2 = street1.strip()
            street1 = building_name
            building_name = ""
        # Otherwise handle normally
        else:
            # If building name is empty, move street1 to building name
            if not building_name.strip():
                building_name = street1
            else:
                # If there's already a building name, append street1 to building name if it looks like a building name
                if not re.search(r"\d", street1): # No numbers usually means it's not a street address
                    building_name = building_name + " " + street1 if building_name else street1
            
            street1 = street2
            street2 = ""

    # --- Extract suite numbers ---
    street1, street2 = extract_suite(street1, street2, suite_keywords, suite_trailing_pattern)
    
    # --- Fix numeric building name (e.g., 314) ---
    if building_name.strip().isdigit() and not street1.strip():
        street1 = building_name + " " + street2 if street2 else building_name
        building_name = ""
        street2 = ""

    building_name, street2 = extract_suite_in_building(building_name, street2, suite_keywords)

    # --- Final processing pass to fix specific patterns ---
    # Handle case where street1 is empty but building_name has street address pattern
    if not street1 and building_name and re.search(r"^\d+\s+\w+", building_name):
        # Building name starts with street number pattern (e.g., "1078 Great Passage")
        street1 = building_name
        building_name = ""
    
    # Handle case where street1 is just a street type word and building_name has the rest of the address
    elif street1 in street_type_only and building_name and re.search(r"\d+", building_name):
        street1 = building_name + " " + street1
        building_name = ""
    
    # Handle Blvd, St, etc. in street1 when building_name looks like a street address
    elif street1 in ["Blvd", "St", "Rd", "Dr", "Ave", "Ln", "Ct", "Cir", "Pl", "Ter", "Hwy", "Pkwy"] and building_name and re.search(r"\d+", building_name):
        # The street type got separated
        street1 = building_name + street1
        building_name = ""
    
    # --- Handle case where street2 is a street type and should be combined with street1 ---
    if street2 in street_type_only and street1:
        street1 = street1 + " " + street2
        street2 = ""
    
    # --- Final cleanup ---
    street1 = street1.strip()
    street2 = street2.strip()
    building_name = building_name.strip()
    city = city.strip()
    if building_name and re.search(r"^\d+", building_name) and street1.strip() in ["Street", "Terrace", "Road", "Drive", "Avenue", "Lane", "Court", "Circle", "Place", "Boulevard", "Highway"]:
        street2 = street1.strip()
        street1 = building_name
        building_name = ""
    
    # --- Final cleanup ---
    street1 = street1.strip()
    street2 = street2.strip()
    building_name = building_name.strip()
    city = city.strip()

    return pd.Series([street1, street2, building_name, city])

# --- Helper functions ---
def extract_po_box(source, street2, pattern, prefer_existing=""):
    match = re.search(pattern, source, re.IGNORECASE)
    if match:
        if not prefer_existing.strip():
            street2 = match.group(0)
        source = re.sub(pattern, "", source, flags=re.IGNORECASE).strip()
    return source, street2

def handle_plaza_america_cases(building_name, street1, street2):
    """Handles special cases like Plaza America Tower"""
    
    # Check for "Plaza America Tower" type pattern in street1
    plaza_pattern = r"(Plaza\s+\w+\s+Tower\s+[IVX]+)"
    plaza_match = re.search(plaza_pattern, street1, re.IGNORECASE)
    
    if plaza_match:
        # Extract the tower name
        tower_name = plaza_match.group(1)
        
        # Remove the tower name from street1
        street1 = re.sub(plaza_pattern, "", street1, flags=re.IGNORECASE).strip()
        
        # If there's a number address after removing the tower name, it's valid street1
        if re.search(r"^\d+", street1):
            # Move tower name to building_name
            building_name = tower_name
        else:
            # If street2 looks like a proper address and street1 doesn't after removal
            if re.search(r"^\d+", street2) and not re.search(r"^\d+", street1):
                building_name = tower_name
                street1 = street2
                street2 = ""
            else:
                # Keep the original street1 if removing tower name makes it invalid
                building_name = tower_name
    
    # Handle suite/apartment separation in both street fields
    suite_pattern = r"(Ste|Suite|Apt|Unit|#)\s*(\d+\w*)"
    suite_match_street1 = re.search(suite_pattern, street1, re.IGNORECASE)
    
    if suite_match_street1:
        suite_text = suite_match_street1.group(0)
        street1 = re.sub(r"\s*" + re.escape(suite_text) + r"\s*", "", street1).strip()
        
        if not street2:
            street2 = suite_text
    
    return building_name, street1, street2

def extract_suite(street1, street2, suite_keywords, trailing_pattern):
    suite_match = re.search(suite_keywords, street1, re.IGNORECASE)
    if suite_match:
        new_street2 = suite_match.group(0)
        if not street2.strip():
            street2 = new_street2
        street1 = re.sub(suite_keywords, "", street1).strip()
    else:
        trailing_match = re.search(trailing_pattern, street1.strip())
        if trailing_match:
            new_street2 = trailing_match.group(0)
            if not street2.strip():
                street2 = new_street2
            street1 = street1[:street1.rfind(new_street2)].strip()
    return street1, street2

def extract_suite_in_building(building_name, street2, suite_keywords):
    suite_in_building = re.search(suite_keywords, building_name, re.IGNORECASE)
    if suite_in_building:
        if not street2.strip():
            street2 = suite_in_building.group(0)
        building_name = re.sub(suite_keywords, "", building_name).strip()
    return building_name, street2

# --- City cleaning ---
def clean_city(c):
    if pd.isna(c):
        return ""
    c = str(c).strip().upper()
    c = re.sub(r"[^\w\s]", "", c)
    c = re.sub(r"\s+", " ", c)
    return c

def is_valid_format(c):
    return not any([
        c == "",
        c.isdigit(),
        any(x in c for x in ["SUITE", "FLOOR", "PLACE", "UNIT", "BUILDING", "STREET", "DRIVE", "LANE", "BLVD"]),
        len(c.split()) > 4
    ])

def match_city(city):
    cleaned = clean_city(city)
    if is_valid_format(cleaned):
        match = process.extractOne(cleaned.title(), VALID_CITIES, scorer=fuzz.WRatio)
        if match and match[1] >= 75:
            return match[0]
    return ""

def format_address(row):
    parts = [
        str(row.get("Street1", "")),
        str(row.get("City_Corrected", "")),
        str(row.get("State", "")),
        str(row.get("ZIPCODE", ""))
    ]
    return ', '.join([part.strip() for part in parts if part.strip()])

# --- Main Lambda handler ---
def lambda_handler(event, context):
    bucket = "merged-fceda-data"
    input_key = "merged_data/joined_df.csv"
    output_key = "merged_data/standardized_address.csv"

    try:
        obj = s3.get_object(Bucket=bucket, Key=input_key)
        df = pd.read_csv(obj['Body'], dtype=str)

        df = df.replace(pd.NA, "", regex=True)
        df = df.replace("nan", "", regex=False)

        df[["Street1", "Street2", "BuildingName", "City"]] = df.apply(clean_row, axis=1)

        df["City"] = df["City"].apply(match_city)

        for col in ["State", "ZIPCODE"]:
            if col not in df.columns:
                df[col] = ""

        df["Full_Address"] = df.apply(format_address, axis=1)

        column_order = ["EntityID", "Name", "Status", "IncorpDate", "IncorpState", "BuildingName",
                        "Street1", "Street2", "City", "State", "BusinessType", "ZIPCODE",
                        "Full_Address", "Shape__Area", "Shape__Length"]
        df = df[[col for col in column_order if col in df.columns]]

        try:
            s3.delete_object(Bucket=bucket, Key=output_key)
        except s3.exceptions.NoSuchKey:
            pass

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=output_key, Body=csv_buffer.getvalue())

        return {
            'statusCode': 200,
            'body': json.dumps(f"Standardized address file saved to s3://{bucket}/{output_key}")
        }

    except s3.exceptions.NoSuchKey:
        return {
            'statusCode': 404,
            'body': json.dumps(f"Input file {input_key} not found in bucket {bucket}")
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }


fceda-als-function

import json
import boto3
import pandas as pd
import io
from datetime import datetime
 
def lambda_handler(event, context):
    """
    This Lambda function:
    - Geocodes all Fairfax County businesses.
    - Input file (standardized_address.csv) already contains only Fairfax County businesses, pre-filtered during earlier cleaning stages.
    - No additional city filtering is needed here.
    - Uses AWS Location Service (ALS) to retrieve Latitude and Longitude for addresses.
    """
 
    # Connect to AWS services
    s3 = boto3.client('s3')
    location = boto3.client('location')
 
    # SETTINGS (Update place_index_name before deploying)
    place_index_name = 'FairfaxIndex' # Your AWS Location Service Place Index Name
    bucket_name = 'merged-fceda-data'
    joined_df_key = 'merged_data/standardized_address.csv'
    date_str = datetime.now().strftime('%m_%d')
    output_key = f'geocoded/geocoded_ready.csv'
    final_geocoded_key = 'geocoded/geocoded_starred.csv'
 
    # CONTROL: Should we geocode everything from scratch?
    force_full_regeocode = False  # Set to True for a full geocode refresh (example: every 6 months)
 
    try:
        # Step 1: Load the cleaned and filtered Fairfax County businesses
        joined_obj = s3.get_object(Bucket=bucket_name, Key=joined_df_key)
        df_current = pd.read_csv(io.BytesIO(joined_obj['Body'].read()), encoding='utf-8-sig')
        df_current.columns = df_current.columns.str.strip()
        df_current['EntityID'] = df_current['EntityID'].astype(str).str.strip()
 
        # Step 2: Try loading previously geocoded businesses
        try:
            final_obj = s3.get_object(Bucket=bucket_name, Key=final_geocoded_key)
            df_final = pd.read_csv(io.BytesIO(final_obj['Body'].read()), encoding='utf-8-sig')
            df_final.columns = df_final.columns.str.strip()
            df_final['EntityID'] = df_final['EntityID'].astype(str).str.strip()
        except s3.exceptions.NoSuchKey:
            # If no previous geocoded file found, start fresh
            df_final = pd.DataFrame(columns=[
        'EntityID', 'Name', 'Status', 'IncorpDate', 'IncorpState',
        'BuildingName', 'Street1', 'Street2', 'City', 'State',
        'BusinessType', 'ZIPCODE', 'Full_Address', 'Shape__Area',
        'Shape__Length', 'Latitude', 'Longitude'
    ])
        # Step 3: Decide whether to geocode everything or only new businesses
        if force_full_regeocode:
            df_to_geocode = df_current.copy()
            df_to_geocode['Latitude'] = None
            df_to_geocode['Longitude'] = None
        else:
            existing_ids = set(df_final['EntityID'])
            df_to_geocode = df_current[~df_current['EntityID'].isin(existing_ids)].copy()
  
        # Step 5: Geocode addresses using AWS Location Service
        latitudes = []
        longitudes = []
 
        for idx, row in df_to_geocode.iterrows():
            address = row['Full_Address']
            try:
                response = location.search_place_index_for_text(
                    IndexName=place_index_name,
                    Text=address
                )
                if response['Results']:
                    coords = response['Results'][0]['Place']['Geometry']['Point']
                    longitudes.append(coords[0])  # Longitude comes first
                    latitudes.append(coords[1])   # Then Latitude
                else:
                    longitudes.append(None)
                    latitudes.append(None)
            except Exception:
                # If geocoding fails, save None for that record
                longitudes.append(None)
                latitudes.append(None)
 
        # Step 6: Assign geocode results to the dataset
        df_to_geocode['Latitude'] = latitudes
        df_to_geocode['Longitude'] = longitudes

        #LOG SUMMARY
        total_input = len(df_current)
        total_attempted = len(df_to_geocode)
        already_geocoded = total_input - total_attempted
        successful = df_to_geocode['Latitude'].notna().sum()
        failed = df_to_geocode['Latitude'].isna().sum()
        remaining_to_geocode = failed

        #Build log message
        summary_log = (
            f"Geocoding Summary Report\n"
            f"Total records in standardized_address.csv     : {total_input}\n"
            f"Already geocoded records (skipped)            : {already_geocoded}\n"
            f"Records attempted for geocoding               : {total_attempted}\n"
            f"Successfully geocoded records                 : {successful}\n"
            f"Failed geocodes                               : {failed}\n"
            f"Remaining records to geocode in future runs   : {remaining_to_geocode}\n"
        )

        #Save summary log to S3
        summary_key = f'geocoded-logs/geocode_summary_{date_str}.txt'
        s3.put_object(
            Bucket=bucket_name,
            Key=summary_key,
            Body=summary_log.encode('utf-8')
        )

        
        # Step 7: Update the master geocoded file
        if force_full_regeocode:
            df_final_updated = df_to_geocode[['EntityID', 'Name', 'Status', 'IncorpDate', 'IncorpState', 'BuildingName', 'Street1', 'Street2', 'City', 'State', 'BusinessType', 'ZIPCODE', 'Full_Address', 'Shape__Area', 'Shape__Length','Latitude', 'Longitude']]
        else:
            df_new = df_to_geocode[['EntityID', 'Name', 'Status', 'IncorpDate', 'IncorpState', 'BuildingName', 'Street1', 'Street2', 'City', 'State', 'BusinessType', 'ZIPCODE', 'Full_Address', 'Shape__Area', 'Shape__Length','Latitude', 'Longitude']].dropna()
            df_final_updated = pd.concat([df_final, df_new]).drop_duplicates(subset=['EntityID'])
 
        # Step 8: Merge the updated geocodes back into the full business list
        df_merged = pd.merge(df_current, df_final_updated[['EntityID', 'Latitude', 'Longitude']], on='EntityID', how='left')

 
        # Step 9: Save today's geocoded snapshot
        buffer_output = io.BytesIO()
        df_merged.to_csv(buffer_output, index=False)
        buffer_output.seek(0)
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=buffer_output.getvalue())
 
        # Step 10: Save updated cumulative master file
        buffer_final = io.BytesIO()
        df_final_updated[['EntityID', 'Latitude', 'Longitude']].to_csv(buffer_final, index=False)
        buffer_final.seek(0)
        s3.put_object(Bucket=bucket_name, Key=final_geocoded_key, Body=buffer_final.getvalue())
 
        return {
            'statusCode': 200,
            'body': json.dumps(f"Success. Geocoded data saved to {output_key} and updated {final_geocoded_key}.")
        }
 
    except Exception as e:
        # If an error happens, capture it and return it
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error occurred: {str(e)}")
        }


fceda-merge-officers
import json
import boto3
import pandas as pd
import io
from rapidfuzz import fuzz

s3 = boto3.client('s3')

def is_relevant_title(title):
    title = str(title).lower()
    if any(ex in title for ex in ['secr', 'vice', 'assistant', 'past', 'elect', 'trustee', 'officer', 'chairman']):
        return False
    score_president = fuzz.partial_ratio(title, 'president')
    score_ceo = fuzz.partial_ratio(title, 'ceo')
    score_owner = fuzz.partial_ratio(title, 'owner')
    return score_president > 80 or score_ceo > 80 or score_owner > 80

def lambda_handler(event, context):
    # S3 bucket and keys
    officers_bucket = 'cleaned-fceda-data'
    officers_key = 'cleaned_data/Officer.csv'
    metadata_bucket = 'cleaned-fceda-data'
    metadata_key = 'fairfax_filtered/fairfax_filtered.csv'
    output_bucket = 'merged-fceda-data'
    output_key = 'merged_officers_data/fairfax_with_officers.csv'

    # Load officer file
    officers_obj = s3.get_object(Bucket=officers_bucket, Key=officers_key)
    officers = pd.read_csv(io.BytesIO(officers_obj['Body'].read()), dtype=str)

    # Load metadata file
    metadata_obj = s3.get_object(Bucket=metadata_bucket, Key=metadata_key)
    metadata = pd.read_csv(io.BytesIO(metadata_obj['Body'].read()), dtype=str)

    # Filter officers by relevant titles
    filtered_officers = officers[officers['OfficerTitle'].apply(is_relevant_title)]

    # Merge metadata with filtered officers
    merged = pd.merge(
        metadata,
        filtered_officers,
        on='EntityID',
        how='left',
        suffixes=('', '_officer')
    )

    # Rename officer columns from the merged dataframe
    merged = merged.rename(columns={
        'OfficerFirstName_officer': 'OfficerFirstName',
        'OfficerLastName_officer': 'OfficerLastName',
        'OfficerTitle_officer': 'OfficerTitle'
    })

    # Reorder columns: move officer fields to the end
    officer_cols = ['OfficerLastName', 'OfficerFirstName', 'OfficerTitle']
    other_cols = [col for col in merged.columns if col not in officer_cols]
    merged = merged[other_cols + officer_cols]

    # Select only required columns (ensure all exist)
    columns_to_keep = [
        "EntityID", "Name", "Status", "IncorpDate", "IncorpState", "BuildingName", "Street1", "Street2",
        "City", "State", "BusinessType", "ZIPCODE", "Shape__Area", "Shape__Length",
        "Full_Address", "Latitude", "Longitude", 'OfficerFirstName', 'OfficerLastName', 'OfficerTitle'
    ]
    available_columns = [col for col in columns_to_keep if col in merged.columns]
    merged = merged[available_columns]

    # Save result to S3
    csv_buffer = io.StringIO()
    merged.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())

    # Return success message
    return {
        'statusCode': 200,
        'body': json.dumps(f"✅ {len(merged)} rows saved to s3://{output_bucket}/{output_key}")
    }



fceda-merge-building-type
import json
import boto3
import pandas as pd
import io
import re

s3 = boto3.client('s3')

# === Street abbreviation mapping ===
abbreviation_map = {
    r"\bRDG\b": "RIDGE",
    r"\bRI\b": "RIDGE",
    r"\bRD\b": "ROAD",
    r"\bST\b": "STREET",
    r"\bAVE\b": "AVENUE",
    r"\bBLVD\b": "BOULEVARD",
    r"\bCTR\b": "CENTER",
    r"\bLN\b": "LANE",
    r"\bDR\b": "DRIVE",
    r"\bPL\b": "PLACE",
    r"\bPKWY\b": "PARKWAY",
    r"\bHWY\b": "HIGHWAY",
}

def normalize_address(addr):
    addr = str(addr).upper()
    addr = re.sub(r'[^\w\s]', '', addr)  # Remove punctuation
    for pattern, replacement in abbreviation_map.items():
        addr = re.sub(pattern, replacement, addr)
    return addr.strip()

def lambda_handler(event, context):
    # === Define S3 paths ===
    business_bucket = 'merged-fceda-data'
    business_key = 'merged_officers_data/fairfax_with_officers.csv'
    
    building_bucket = 'cleaned-fceda-data'
    building_key = 'cleaned_data/Building_Type_of_Construction.csv'
    
    output_bucket = 'merged-fceda-data'
    output_key = 'merged_fairfax_building/fairfax_with_building.csv'
    
    # === Load building dataset ===
    building_obj = s3.get_object(Bucket=building_bucket, Key=building_key)
    building_df = pd.read_csv(io.BytesIO(building_obj['Body'].read()))
    building_df.columns = building_df.columns.str.strip()
    
    # === Load business dataset ===
    business_obj = s3.get_object(Bucket=business_bucket, Key=business_key)
    business_df = pd.read_csv(io.BytesIO(business_obj['Body'].read()))
    business_df.columns = business_df.columns.str.strip()
    
    # === Apply normalization ===
    building_df["Address_clean"] = building_df["Address:"].fillna("").apply(normalize_address)
    business_df["Street1_clean"] = business_df["Street1"].fillna("").apply(normalize_address)
    
    # === Merge on cleaned street fields ===
    merged = pd.merge(
        business_df,
        building_df[["Address_clean", "Cleaned Group(s)"]],
        left_on="Street1_clean",
        right_on="Address_clean",
        how="left"
    )
    
    # Drop helper columns
    merged = merged.drop(columns=["Street1_clean", "Address_clean"])
    
    # Filter only B and F (optional — same as notebook)
    merged_filtered = merged[merged["Cleaned Group(s)"].isin(["B", "F"])]
    
    # Remove duplicates
    merged_filtered = merged_filtered.drop_duplicates()
    
    # Save the result back to S3
    csv_buffer = io.StringIO()
    merged_filtered.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    
    # Return basic success info
    return {
        'statusCode': 200,
        'body': json.dumps(f"✅ {len(merged_filtered)} rows saved to s3://{output_bucket}/{output_key}")
    }



