import geopandas as gpd
import geopy.exc
from shapely.geometry import Point
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from opencage.geocoder import OpenCageGeocode
from googlemaps import Client as GoogleMaps
import os
import time
from dotenv import load_dotenv
from censusgeocode import CensusGeocode
from Geocoder import *

# GeocoderBatch - A GIS Batch Encoder using free/trial services
# Copyright (C) 2025 Jeffrey Hu
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Modified from the original GeocoderBatch
# Throw in your environmental path here to your API keys
load_dotenv(dotenv_path='path_to_your_dotenv_file')

opencage = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
gmaps = GoogleMaps(key=os.getenv('GOOGLE_MAPS_API_KEY'))
census = CensusGeocode()


geocoding_successes = {'census': 0, 'opencage': 0, 'nominatim': 0}
geocoding_failures = {'census': 0, 'opencage': 0, 'nominatim': 0}


def prepare_census_batch(df, address_col, city_col, state_col, zip_col):
    """
    Prepare data for Census batch geocoding.
    Creates a CSV file in the required format: Unique ID, Street Address, City, State, ZIP
    """
    # Create census format dataframe
    # Example below
    census_df = pd.DataFrame({
        'id': range(1, len(df)+1),  # Unique ID
        'address': df[address_col].str.strip(),
        'city': df[city_col].str.strip(),
        'state': df[state_col].str.strip(),
        'zip': df[zip_col].astype(str).str.strip()
    })

    # # Save without headers in Census format
    # temp_file = 'census_batch_addresses.csv'
    # census_df.to_csv(temp_file, header=False, index=False)
    # return temp_file, census_df['id']

    # Error logging
    # Diagnostic prints
    print("\nSample of addresses being sent to Census:")
    print(census_df.head())
    print("\nChecking for any missing values:")
    print(census_df.isnull().sum())

    # Check zip code format
    print("\nZip code sample and length check:")
    print(census_df['zip'].head())
    print("Zip lengths:", census_df['zip'].str.len().value_counts())

    # Save with diagnostic output
    temp_file = 'census_batch_addresses.csv'
    census_df.to_csv(temp_file, header=False, index=False)

    # Read back and show exactly what's in the file
    print("\nActual content of saved CSV (first 5 lines):")

    with open(temp_file, 'r') as f:
        print(f.read(500))

    return temp_file, census_df['id']


def prepare_census_batch_limit(df, address_col, city_col, state_col, zip_col, batch_size=5000, year=None, output_folder=None):
    """
    Prepare data for Census batch geocoding in smaller batches.
    Creates CSV files in the required format: Unique ID, Street Address, City, State, ZIP

    Parameters:
    df: DataFrame containing address data
    address_col: Column name for street address
    city_col: Column name for city
    state_col: Column name for state
    zip_col: Column name for ZIP code
    batch_size: Size of each batch (default 5000)
    """

    df['BatchID'] = range(1, len(df) + 1)  # 1-based indexing, saving to the original dataframe

    # Create census format dataframe
    census_df = pd.DataFrame({
        'id': df['BatchID'],
        'address': df[address_col].str.strip(),
        'city': df[city_col].str.strip(),
        'state': df[state_col].str.strip(),
        'zip': df[zip_col].astype(str).str.strip()
    })

    # Split into batches and save
    num_batches = (len(census_df) + batch_size - 1) // batch_size
    batch_files = []

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(census_df))
        batch = census_df.iloc[start_idx:end_idx]

        # Create filename with year and ID range
        if year:
            filename = f'census_batch_{year}_{batch.id.min()}_{batch.id.max()}.csv'
        else:
            filename = f'census_batch_{batch.id.min()}_{batch.id.max()}.csv'

        # Save batch
        if output_folder:
            batch.to_csv(rf'{output_folder}/{filename}', header=False, index=False)
        else:
            batch.to_csv(filename, header=False, index=False)

        batch_files.append(filename)

    return batch_files, df['BatchID']


def process_census_results(results, original_df, id_column='ID'):
    """
    Process Census batch results with updated key structure using BatchID to match rows
    """
    print("\nAnalyzing Census batch results:")
    print(f"Number of results: {len(results) if results else 0}")

    for result in results:
        if result and 'lat' in result and 'lon' in result and result['match']:
            # Get the BatchID from the result
            batch_id = int(result['id'])

            # Update the row where BatchID matches
            mask = original_df[id_column] == batch_id
            original_df.loc[mask, 'geometry'] = Point(result['lon'], result['lat'])
            original_df.loc[mask, 'geocoding_service'] = 'census'
            original_df.loc[mask, 'match_score'] = result['matchtype']
            original_df.loc[mask, 'latitude'] = result['lat']
            original_df.loc[mask, 'longitude'] = result['lon']
            geocoding_successes['census'] += 1
        else:
            geocoding_failures['census'] += 1
            print(f"Failed address: {result.get('address', 'Unknown')}")
            if result:
                print(f"Match status: {result.get('match', 'Unknown')}")

    return original_df


def main():
    breakdown_folder = rf''
    id_folder = rf''
    output_folder = rf''
    geocode_folder = rf''

    # Main process
    for file_name in os.listdir(breakdown_folder):
        if file_name.endswith('.csv'):
            df = pd.read_csv(rf'{breakdown_folder}\{file_name}', low_memory=False)

            year = os.path.splitext(file_name)[0][-4:]

            # Street level address join
            # Join any parts here that needed to be joined, in the case that the full address is separated out
            street_parts = [
                df[''],
                df[''],
                df['']
            ]

            df['Full_Address'] = (pd.Series([' '.join(str(x) for x in row if pd.notna(x) and str(x).strip() != '')
                                             for row in zip(*street_parts)])
                                  .str.strip())

            batch_files, id_map = prepare_census_batch_limit(df, '', '',
                                                             '', '',
                                                             year=year, output_folder=output_folder)

            original_output = os.path.join(id_folder, f'{file_name}')
            df.to_csv(original_output, index=False)

            print(f"Created batch files: {batch_files}")
            print(f"Saved original file with BatchID: {original_output}")


    for file_name in os.listdir(output_folder):

        # 'Create' an address df for consistency
        addresses_df = pd.read_csv(rf'{output_folder}\{file_name}', header=None,
                                   names=[])  # Add column names relating to the address file
        addresses_df['geometry'] = None
        addresses_df['geocoding_service'] = None
        addresses_df['match_score'] = None
        addresses_df['latitude'] = None
        addresses_df['longitude'] = None

        census_file = rf'{output_folder}\{file_name}'

        if verify_census_file(census_file):
            print("File verification successful, proceeding with batch geocoding...")
        else:
            print("File verification failed!")
            continue


        # Prepare and run Census batch geocoding
        census_file, id_map = prepare_census_batch(addresses_df)


        print("Waiting 15 seconds before sending batch request...")
        print("Gotta be nice to the people who are letting us do this for free probably")

        time.sleep(15)  # Add delay before batch processing

        try:
            print("Running Census batch geocoding...")
            start_time = time.time()

            census_results = census.addressbatch(census_file)
            # Add ID column
            addresses_df = process_census_results(census_results, addresses_df, id_column='batch_id')

            end_time = time.time()
            elapsed_time = round(end_time - start_time, 2)

            print(f"Census batch geocoding complete in {elapsed_time} seconds.")

            print(f"Census batch geocoding complete. Successes: {geocoding_successes['census']}, "
                  f"Failures: {geocoding_failures['census']}")
        except Exception as e:
            print(f"Exception during Census batch geocoding: {str(e)}")
            print(f"Exception type: {type(e)}")
            if hasattr(e, 'response'):
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.content}")

        # Process remaining addresses with backup services
        # This is quite slow with only free services without batch services
        # fair warning if you have a lot of missing addresses
        addresses_df = geocode_remaining_addresses(addresses_df)

        # Create GeoDataFrame and save results
        addresses_df.to_csv(rf'{geocode_folder}\{file_name}', index=False)

        # Print final statistics
        for service in ['census', 'opencage', 'nominatim']:
            print(f"\n{service.capitalize()} geocoding:")
            print(f"Successes: {geocoding_successes[service]}")
            print(f"Failures: {geocoding_failures[service]}")

        # if os.path.exists(census_file):
        #     os.remove(census_file)

        # print("Breaking the test")
        # break


if __name__ == '__main__':
    main()