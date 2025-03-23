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


# Tech Debt, clean this up some time
# Future:
# Run a batch to Census, only correct if it's missing
# Census will be worse for more historical data, i.e. if street names change


# Load API keys (don't print) from environment variables
# You will need your own OpenCage and GoogleMaps API Key
load_dotenv(dotenv_path='path_to_your_dotenv_file')

opencage = OpenCageGeocode(os.getenv('OPENCAGE_API_KEY'))
gmaps = GoogleMaps(key=os.getenv('GOOGLE_MAPS_API_KEY'))
census = CensusGeocode()


geocoding_successes = {'census': 0, 'opencage': 0, 'nominatim': 0}
geocoding_failures = {'census': 0, 'opencage': 0, 'nominatim': 0}


def geocode_address_census(address):
    threshold = 85
    try:
        result = census.onelineaddress(address)

        if result and 'coordinates' in result[0]:
            coords = result[0]['coordinates']
            matchedAddress = result[0]['matchedAddress']

            print(f"Census geocoding successful for address: {address}")
            geocoding_successes['census'] += 1
            return Point(coords['x'], coords['y']), 'census', matchedAddress
        else:
            print(f"Census geocoding failed for address: {address}")
            geocoding_failures['census'] += 1
    except Exception as e:
        print(f"Census geocoding error for address: {address}. Error: {str(e)}")
        geocoding_failures['census'] += 1
    return None, None, None


def geocode_address_opencage(address):
    threshold = 7
    try:
        result = opencage.geocode(address)
        if result and len(result):
            location = result[0]['geometry']
            confidence = result[0]['confidence']
            if confidence >= threshold:
                print(f"OpenCage geocoding successful for address: {address}")
                geocoding_successes['opencage'] += 1
                return Point(location['lng'], location['lat']), 'opencage', confidence
            else:
                print(f"OpenCage geocoding confidence is too low, below threshold. Confidence: {confidence}")
                geocoding_failures['opencage'] += 1
        else:
            print(f"OpenCage geocoding failed for address: {address}")
            geocoding_failures['opencage'] += 1
    except Exception as e:
        print(f"OpenCage geocoding error for address: {address}. Error: {str(e)}")
        geocoding_failures['opencage'] += 1
    return None, None, None


def geocode_address_nominatim(address):
    # Replace with your own description and email in this format (please don't use my email)
    geolocator = Nominatim(user_agent="your-app-name <your-email@example.com>")
    try:
        location = geolocator.geocode(address)
        if location:
            print(f"Nominatim geocoding successful for address: {address}")
            geocoding_successes['nominatim'] += 1
            return Point(location.longitude, location.latitude), 'nominatim', 'N/A'
        else:
            print(f"Nominatim geocoding failed for address: {address}")
            geocoding_failures['nominatim'] += 1
    except (GeocoderTimedOut, geopy.exc.GeocoderUnavailable):
        print(f"Nominatim geocoding request failed for address: {address}")
        geocoding_failures['nominatim'] += 1
    return None, None, None


def geocode_address(address):
    # Try the US Census Geocoder first
    # Highest limits, allows multibatching and multiple calls for future use, doesn't need an API key
    result, service, match = geocode_address_census(address)
    if result:
        return result, service, match

    # Try the OpenCage next
    result, service, match = geocode_address_opencage(address)
    if result:
        return result, service, match

    # Try Nominatim (OpenStreetMap) next
    result, service, match = geocode_address_nominatim(address)
    if result:
        return result, service, match

    # If all services fail, return None
    print(f'All services failed on {address}')
    return None, None, None


def prepare_census_batch(df):
    """
    Prepare data for Census batch geocoding.
    Creates a CSV file in the required format: Unique ID, Street Address, City, State, ZIP
    """
    # Create census format dataframe
    census_df = pd.DataFrame({
        'id': range(1, len(df)+1),  # Unique ID
        'address': df['StudentAddress'].str.strip(),
        'city': df['StudentCity'].str.strip(),
        'state': df['StudentState'].str.strip(),
        'zip': df['StudentZip'].astype(str).str.strip()
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


def process_census_results(results, original_df):
    """
    Process Census batch results with updated key structure
    """
    print("\nAnalyzing Census batch results:")
    print(f"Number of results: {len(results) if results else 0}")

    for result in results:
        if result and 'lat' in result and 'lon' in result and result['match']:
            idx = int(result['id']) - 1  # Census returns id as string, subtract one to get the correct 0 idx row in pandas
            original_df.loc[idx, 'geometry'] = Point(result['lon'], result['lat'])
            original_df.loc[idx, 'geocoding_service'] = 'census'
            original_df.loc[idx, 'match_score'] = result['matchtype']
            geocoding_successes['census'] += 1
        else:
            geocoding_failures['census'] += 1
            print(f"Failed address: {result.get('address', 'Unknown')}")
            if result:
                print(f"Match status: {result.get('match', 'Unknown')}")

    return original_df


def geocode_remaining_addresses(df):
    """
    Geocode addresses that failed with Census using backup services
    """
    mask = df['geometry'].isna()
    if not mask.any():
        return df

    for idx, row in df[mask].iterrows():
        address = f"{row['StudentAddress']}, {row['StudentCity']}, {row['StudentState']} {row['StudentZip']}"

        # Try OpenCage
        result, service, match = geocode_address_opencage(address)
        if result:
            df.loc[idx, 'geometry'] = result
            df.loc[idx, 'geocoding_service'] = service
            df.loc[idx, 'match_score'] = match
            continue

        # Try Nominatim
        result, service, match = geocode_address_nominatim(address)
        if result:
            df.loc[idx, 'geometry'] = result
            df.loc[idx, 'geocoding_service'] = service
            df.loc[idx, 'match_score'] = match

    return df


def verify_census_file(filename):
    """
    Verify the census file exists and is readable
    """
    try:
        file_size = os.path.getsize(filename)
        print(f"\nCensus file verification:")
        print(f"File exists: {os.path.exists(filename)}")
        print(f"File size: {file_size} bytes")

        with open(filename, 'r') as f:
            line_count = sum(1 for line in f)
        print(f"Number of lines in file: {line_count}")

        # Read and print first few lines
        with open(filename, 'r') as f:
            print("\nFirst 5 lines of census file:")
            for i, line in enumerate(f):
                if i < 5:
                    print(line.strip())
                else:
                    break

        return True
    except Exception as e:
        print(f"Error verifying census file: {str(e)}")
        return False


def main():
    # Main process
    # Add path here to address file that needs to be geocoded
    addresses_df = pd.read_csv(r'path_to_input.csv', low_memory=False)

    # Initialize geometry column
    addresses_df['geometry'] = None
    addresses_df['geocoding_service'] = None
    addresses_df['match_score'] = None

    # Prepare and run Census batch geocoding
    census_file, id_map = prepare_census_batch(addresses_df)


    if verify_census_file(census_file):
        print("File verification successful, proceeding with batch geocoding...")
    else:
        print("File verification failed!")


    print("Waiting 10 seconds before sending batch request...")
    time.sleep(10)  # Add delay before batch processing

    try:
        print("Running Census batch geocoding...")
        census_results = census.addressbatch(census_file)
        addresses_df = process_census_results(census_results, addresses_df)
        print(f"Census batch geocoding complete. Successes: {geocoding_successes['census']}, "
              f"Failures: {geocoding_failures['census']}")
    except Exception as e:
        print(f"Exception during Census batch geocoding: {str(e)}")
        print(f"Exception type: {type(e)}")
        if hasattr(e, 'response'):
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.content}")


    # Process remaining addresses with backup services
    addresses_df = geocode_remaining_addresses(addresses_df)

    print(addresses_df.head(100))

    # Create GeoDataFrame and save results
    gdf_all = gpd.GeoDataFrame(addresses_df.dropna(subset=['geometry']), geometry='geometry', crs="EPSG:4326")

    # Add output name here
    addresses_df.to_csv('path_to_output.csv')

    # Print final statistics
    for service in ['census', 'opencage', 'nominatim']:
        print(f"\n{service.capitalize()} geocoding:")
        print(f"Successes: {geocoding_successes[service]}")
        print(f"Failures: {geocoding_failures[service]}")


    # if os.path.exists(census_file):
    #     os.remove(census_file)


if __name__ == '__main__':
    main()