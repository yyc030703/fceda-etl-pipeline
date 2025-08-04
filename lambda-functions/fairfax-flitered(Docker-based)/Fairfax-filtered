import boto3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import tempfile
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'gmu-fceda-project'
    shapefile_prefix = 'Fairfax_County_Boundary/'
    business_bucket = 'merged-fceda-data'
    output_bucket = 'cleaned-fceda-data'
    output_key = 'fairfax_filtered.csv'

    business_key = 'geocoded/geocoded_ready.csv'  # ← Always use this fixed name

    with tempfile.TemporaryDirectory() as tmpdir:
        # Download business CSV
        business_local_path = os.path.join(tmpdir, 'geocoded_ready.csv')
        s3.download_file(business_bucket, business_key, business_local_path)
        businesses = pd.read_csv(business_local_path)

        if "Latitude" not in businesses.columns or "Longitude" not in businesses.columns:
            raise ValueError("Check your latitude and longitude column names!")

        businesses = businesses.dropna(subset=['Latitude', 'Longitude'])
        geometry = [Point(xy) for xy in zip(businesses["Longitude"], businesses["Latitude"])]
        businesses_gdf = gpd.GeoDataFrame(businesses, geometry=geometry, crs="EPSG:4326")

        # Download shapefile components
        shapefile_components = ['.shp', '.shx', '.dbf', '.prj']
        for ext in shapefile_components:
            s3.download_file(bucket_name, shapefile_prefix + 'Fairfax_County_Boundary' + ext,
                             os.path.join(tmpdir, 'Fairfax_County_Boundary' + ext))

        fairfax_shapefile_path = os.path.join(tmpdir, 'Fairfax_County_Boundary.shp')
        fairfax_county = gpd.read_file(fairfax_shapefile_path)

        # Match CRS if needed
        if businesses_gdf.crs != fairfax_county.crs:
            businesses_gdf = businesses_gdf.to_crs(fairfax_county.crs)

        # Optional: exclude Fairfax City
        if "City" in fairfax_county.columns:
            fairfax_county = fairfax_county[fairfax_county["City"] != "Fairfax City"]

        # Spatial join
        filtered_businesses = gpd.sjoin(businesses_gdf, fairfax_county, predicate="intersects", how="inner")

        # Drop geometry for CSV
        if 'geometry' in filtered_businesses.columns:
            filtered_businesses = filtered_businesses.drop(columns=['geometry'])

        output_local_path = os.path.join(tmpdir, 'fairfax_filtered.csv')
        filtered_businesses.to_csv(output_local_path, index=False)
        s3.upload_file(output_local_path, output_bucket, output_key)
        print(f"Filtered data uploaded to s3://{output_bucket}/{output_key}")

    return {
        'statusCode': 200,
        'body': f'Filtered data with all columns saved to s3://{output_bucket}/{output_key}'
    }
