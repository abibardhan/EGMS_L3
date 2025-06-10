import os
import csv
from tqdm import tqdm
from geopy.geocoders import Nominatim
import pyproj
from time import sleep

# Configuration
NAMES_DATASETS_DIR = "Point_locations"

# dataset CSV path
INPUT_CSV_PATH = "Point_downloads/EGMS_L3_E30N33_100km_U_2019_2023_1.csv"

def init_transformer():
    """Initialize coordinate transformer from ETRS89/LAEA Europe to WGS84"""
    try:

        return pyproj.Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
    except Exception as e:
        print(f"Warning: Could not initialize coordinate transformer: {e}")
        return None

def convert_coordinates(easting, northing, transformer=None):
    """Convert easting/northing coordinates to latitude/longitude"""
    if transformer is None:
        # Fallback to initialize transformer if not provided
        transformer = init_transformer()
        if transformer is None:
            return None, None
    
    try:
        # Transform from projected coordinates to lat/lon
        lon, lat = transformer.transform(float(easting), float(northing))
        return lat, lon
    except Exception as e:
        print(f"Error converting coordinates: {e}")
        return None, None

def get_location_name(latitude, longitude):
    """Get location name for the given coordinates"""
    if latitude is None or longitude is None:
        return "Unknown location"
        
    geolocator = Nominatim(user_agent="egms-cli")
    try:
        location = geolocator.reverse((latitude, longitude), exactly_one=True)
        if location:
            address = location.raw.get('address', {})
            city = address.get('city', address.get('town', address.get('village', '')))
            country = address.get('country', '')
            if city and country:
                return f"{city}, {country}"
            return location.address
        return "Unknown location"
    except Exception as e:
        print(f"Error in geocoding: {e}")
        return "Geocoding error"

def enrich_csv_with_locations(input_file):
    """Add location names to each point in the CSV file"""
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return
    
    # Create the names datasets directory if it doesn't exist
    os.makedirs(NAMES_DATASETS_DIR, exist_ok=True)
    
    # Get the base filename without path and extension
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(NAMES_DATASETS_DIR, f"{base_filename}_locations.csv")
    
    # Initialize coordinate transformer
    transformer = init_transformer()
    if transformer is None:
        print("Warning: Using approximate coordinate conversion")
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Read header
            header = next(reader)
            
            # Process headers (normalize to lowercase for case-insensitive matching)
            header_lower = [col.lower() for col in header]
            
            # Find column indices
            easting_idx = header_lower.index('easting') if 'easting' in header_lower else None
            northing_idx = header_lower.index('northing') if 'northing' in header_lower else None
            
            if easting_idx is None or northing_idx is None:
                print(f"Required columns not found. Available columns: {header}")
                return
                
            print(f"Found columns: easting={header[easting_idx]}, northing={header[northing_idx]}")
            
            # Create new header with location added after northing
            new_header = header.copy()
            new_header.insert(northing_idx + 1, 'location')
            
            # Write the new header
            writer.writerow(new_header)
            
            # Process each row
            for row in tqdm(reader, desc="Processing coordinates"):
                easting = row[easting_idx]
                northing = row[northing_idx]
                
                # Convert from easting/northing to lat/lon
                lat, lon = convert_coordinates(easting, northing, transformer)
                
                # Get location name using converted coordinates
                location = get_location_name(lat, lon)
                sleep(0.5)  # 0.5s delay between requests to avoid overwhelming the service
                
                # Copy original row and insert location after northing
                new_row = row.copy()
                new_row.insert(northing_idx + 1, location)
                
                # Write the updated row
                writer.writerow(new_row)
                
        print(f"Location dataset saved as: {output_file}")
    
    except Exception as e:
        print(f"Error processing CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=== EGMS Location Name Generator ===")
    print(f"Using input file: {INPUT_CSV_PATH}")
    
    if not os.path.exists(INPUT_CSV_PATH):
        print(f"Error: Input file not found at {INPUT_CSV_PATH}")
        print("Please download the file first or update the INPUT_CSV_PATH")
    else:
        print("Adding location names to points...")
        enrich_csv_with_locations(INPUT_CSV_PATH) 