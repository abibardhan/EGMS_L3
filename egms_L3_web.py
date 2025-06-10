import streamlit as st
import zipfile
import os
from io import BytesIO
from time import sleep
import csv
from curl_cffi import requests as curl_requests
import pyproj
import math
import pandas as pd
import glob
from geopy.geocoders import Nominatim

## Run in web
# .\env\Scripts\activate
# streamlit run egms_L3_web.py

# Configuration
BASE_URL = "https://egms.land.copernicus.eu/insar-api/archive/download/EGMS_L3_E{e}N{n}_100km_{d}_{year}_1.zip?id={id}"
DISPLACEMENTS = ["E", "U"]
DOWNLOAD_BASE = "Point_downloads"
NAMES_DATASETS_DIR = "Point_locations"
DELAY = 3.0  # seconds between requests
DEFAULT_YEAR = "2019_2023"
DEFAULT_ID = "7ce01544f73b4a9780b56f9c96fe4de3"

# Initialize session state variables
if 'download_status' not in st.session_state:
    st.session_state.download_status = ""
if 'current_progress' not in st.session_state:
    st.session_state.current_progress = 0
if 'total_tasks' not in st.session_state:
    st.session_state.total_tasks = 0
if 'enrich_status' not in st.session_state:
    st.session_state.enrich_status = ""

# Coordinate transformation setup - ETRS89 / LAEA Europe (EPSG:3035) to WGS84 (EPSG:4326)
@st.cache_resource
def init_transformer():
    try:
        # EPSG:3035 is ETRS89-extended / LAEA Europe (commonly used for EGMS data)
        # EPSG:4326 is WGS84 (standard latitude/longitude)
        return pyproj.Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
    except Exception as e:
        st.warning(f"Could not initialize coordinate transformer: {e}")
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
        st.error(f"Error converting coordinates: {e}")
        return None, None

def download_tile(e, n, d, year=DEFAULT_YEAR, id=DEFAULT_ID):
    """Download a single tile with given coordinates and displacement type"""
    tile_code = f"E{e}N{n}"
    filename_prefix = f"EGMS_L3_{tile_code}_100km_{d}_{year}_1"
    url = BASE_URL.format(e=e, n=n, d=d, year=year, id=id)
    
    try:
        response = curl_requests.get(url,timeout=600)
        st.session_state.download_status = f"Response for {tile_code} {d}: {response.status_code}"
        
        if response.status_code != 200:
            st.session_state.download_status = f"Failed to download {tile_code} {d}"
            return False
        
        # Create download directory if it doesn't exist
        os.makedirs(DOWNLOAD_BASE, exist_ok=True)
        
        # Read zip from memory
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            for name in z.namelist():
                if name.endswith(".csv") and filename_prefix in name:
                    z.extract(name, path=DOWNLOAD_BASE)
                    st.session_state.download_status = f"Extracted {name}"
                    return True
        
        st.session_state.download_status = f"No matching CSV found in the downloaded zip for {tile_code} {d}"
        return False
    
    except Exception as e:
        st.session_state.download_status = f"Error downloading {tile_code} {d}: {e}"
        return False

def get_location_name(latitude, longitude):
    """Get location name for the given coordinates"""
    if latitude is None or longitude is None:
        return "Unknown location"
        
    geolocator = Nominatim(user_agent="egms-streamlit")
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
        st.warning(f"Error in geocoding: {e}")
        return "Geocoding error"

def list_downloaded_datasets():
    """List all downloaded datasets"""
    if not os.path.exists(DOWNLOAD_BASE):
        return []
    
    datasets = []
    for file in glob.glob(os.path.join(DOWNLOAD_BASE, "*.csv")):
        datasets.append(file)
    
    return datasets

def enrich_dataset_with_locations(input_file, progress_bar):
    """Add location names to each point in the CSV file"""
    if not os.path.exists(input_file):
        st.error(f"File not found: {input_file}")
        return
    
    # Create the names datasets directory if it doesn't exist
    os.makedirs(NAMES_DATASETS_DIR, exist_ok=True)
    
    # Get the base filename without path and extension
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(NAMES_DATASETS_DIR, f"{base_filename}_locations.csv")
    
    # Initialize coordinate transformer
    transformer = init_transformer()
    if transformer is None:
        st.warning("Using approximate coordinate conversion")
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Read header
            header = next(reader)
            
            # Process headers (normalize to lowercase for case-insensitive matching)
            header_lower = [col.lower() for col in header]
            
            # Find column indices
            pid_idx = header_lower.index('pid') if 'pid' in header_lower else None
            easting_idx = header_lower.index('easting') if 'easting' in header_lower else None
            northing_idx = header_lower.index('northing') if 'northing' in header_lower else None
            
            if pid_idx is None or easting_idx is None or northing_idx is None:
                st.error(f"Required columns not found. Available columns: {header}")
                return
                
            st.session_state.enrich_status = f"Found columns: pid={header[pid_idx]}, easting={header[easting_idx]}, northing={header[northing_idx]}"
            
            # Write the new header
            writer.writerow(['point_id', 'easting', 'northing', 'location'])
            
            # Count total rows for progress reporting
            rows = list(reader)
            total_rows = len(rows)
            
            # Reset file pointer
            infile.seek(0)
            next(reader)  # Skip header again
            
            # Process each row
            for i, row in enumerate(rows):
                easting = row[easting_idx]
                northing = row[northing_idx]
                
                # Convert from easting/northing to lat/lon
                lat, lon = convert_coordinates(easting, northing, transformer)
                
                # Get location name using converted coordinates
                location = get_location_name(lat, lon)
                sleep(0.5)  # 0.5s delay between requests to avoid overwhelming the service
                
                # Write only the required columns
                writer.writerow([
                    row[pid_idx], 
                    row[easting_idx], 
                    row[northing_idx],
                    location
                ])
                
                # Update progress
                progress = (i + 1) / total_rows
                progress_bar.progress(progress, text=f"Processing {i+1}/{total_rows} points")
                
        st.success(f"Location dataset saved as: {output_file}")
        return output_file
    
    except Exception as e:
        st.error(f"Error processing CSV: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None

def main():
    st.set_page_config(
        page_title="EGMS Data Tool",
        page_icon="🌍",
        layout="wide"
    )
    
    st.title("🌍 EGMS L3-Data Tool")
    st.markdown("Download and process European Ground Motion Service (EGMS) data")
    st.markdown("Developed by Dr. Abidhan Bardhan and Salmen Abbes")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Download Data", "Add Location Names", "View Data"])
    
    # Download Data Tab
    with tab1:
        st.header("Download EGMS Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            download_type = st.radio(
                "Choose download type",
                ["Single Tile", "Batch Download"],
                horizontal=True
            )
            
            if download_type == "Single Tile":
                n_coord = st.number_input("North", min_value=9, max_value=55, value=31)
                e_coord = st.number_input("East", min_value=9, max_value=65, value=32)
                
                disp_choice = st.radio(
                    "Displacement type",
                    ["E", "U", "Both"],
                    horizontal=True
                )
                
                year = st.selectbox(
                    "Year range",
                    ["2018_2022", "2019_2023", "2020_2024"],
                    index=1  # Default to 2019_2023
                )
                
                id_value = st.text_input("ID", value=DEFAULT_ID)
                
                if st.button("Download Single Tile"):
                    if disp_choice == "Both":
                        displacements = ["E", "U"]
                    else:
                        displacements = [disp_choice]
                    
                    progress_bar = st.progress(0, text="Starting download...")
                    status_text = st.empty()
                    
                    for i, d in enumerate(displacements):
                        progress = i / len(displacements)
                        progress_bar.progress(progress, text=f"Downloading {d} displacement data...")
                        status_text.text(f"Downloading E{e_coord}N{n_coord} {d}...")
                        
                        download_tile(e_coord, n_coord, d, year, id_value)
                        sleep(DELAY)
                        
                        status_text.text(st.session_state.download_status)
                    
                    progress_bar.progress(1.0, text="Download complete!")
            
            else:  # Batch Download
                col1a, col1b = st.columns(2)
                with col1a:
                    min_n = st.number_input("Min North", min_value=9, max_value=55, value=25)
                    min_e = st.number_input("Min East", min_value=9, max_value=65, value=10)
                
                with col1b:
                    max_n = st.number_input("Max North", min_value=9, max_value=55, value=26)
                    max_e = st.number_input("Max East", min_value=9, max_value=65, value=11)
                
                disp_choice = st.radio(
                    "Displacement type (batch)",
                    ["E", "U", "Both"],
                    horizontal=True
                )
                
                year = st.selectbox(
                    "Year range (batch)",
                    ["2018_2022", "2019_2023", "2020_2024"],
                    index=1  # Default to 2019_2023
                )
                
                id_value = st.text_input("ID (batch)", value=DEFAULT_ID)
                
                total_tiles = (max_e - min_e + 1) * (max_n - min_n + 1)
                st.info(f"This will download {total_tiles} {'tiles' if total_tiles > 1 else 'tile'} " + 
                       f"({'x2' if disp_choice == 'Both' else 'x1'} for displacement type)")
                
                if st.button("Start Batch Download"):
                    if disp_choice == "Both":
                        displacements = ["E", "U"]
                    else:
                        displacements = [disp_choice]
                    
                    total_tasks = (max_e - min_e + 1) * (max_n - min_n + 1) * len(displacements)
                    st.session_state.total_tasks = total_tasks
                    
                    progress_bar = st.progress(0, text="Starting batch download...")
                    status_text = st.empty()
                    
                    task_count = 0
                    for e in range(min_e, max_e + 1):
                        for n in range(min_n, max_n + 1):
                            for d in displacements:
                                task_count += 1
                                progress = task_count / total_tasks
                                progress_bar.progress(progress, text=f"Downloading tile {task_count}/{total_tasks}")
                                status_text.text(f"Downloading E{e}N{n} {d}...")
                                
                                download_tile(e, n, d, year, id_value)
                                status_text.text(st.session_state.download_status)
                                sleep(DELAY)
                    
                    progress_bar.progress(1.0, text="Batch download complete!")
        
        with col2:
            st.subheader("Download Status")
            
            # Add explanation about the data
            st.markdown("""
            ### About EGMS Data
            The European Ground Motion Service (EGMS) provides information about ground movements across Europe.
            
            - E: East-West displacement
            - U: Up-Down displacement
            
            The data is organized in 100x100 km tiles, referenced by E and N coordinates.
            """)
            
            # Show list of downloaded datasets
            st.subheader("Downloaded Datasets")
            if st.button("Refresh List"):
                pass  # The list will refresh automatically below
            
            datasets = list_downloaded_datasets()
            if datasets:
                for dataset in datasets:
                    st.text(os.path.basename(dataset))
            else:
                st.info("No datasets downloaded yet")
    
    # Location Names Tab
    with tab2:
        st.header("Add Location Names to Dataset")
        
        datasets = list_downloaded_datasets()
        if not datasets:
            st.warning("No datasets found. Please download data first.")
        else:
            selected_dataset = st.selectbox(
                "Select dataset to process",
                options=datasets,
                format_func=os.path.basename
            )
            
            if st.button("Add Location Information"):
                progress_bar = st.progress(0, text="Starting location processing...")
                
                output_file = enrich_dataset_with_locations(selected_dataset, progress_bar)
                
                if output_file:
                    # Show a sample of the enriched data
                    try:
                        df = pd.read_csv(output_file)
                        st.subheader("Sample of Location Data")
                        st.dataframe(df.head(10))
                    except Exception as e:
                        st.error(f"Error displaying data: {e}")
    
    # View Data Tab
    with tab3:
        st.header("View Data")
        
        # Show both original datasets and location datasets
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Original Datasets")
            original_datasets = list_downloaded_datasets()
            
            if not original_datasets:
                st.info("No original datasets found")
            else:
                selected_original = st.selectbox(
                    "Select original dataset to view",
                    options=original_datasets,
                    format_func=os.path.basename
                )
                
                if selected_original:
                    try:
                        df = pd.read_csv(selected_original)
                        st.dataframe(df.head(100))
                        st.info(f"Total rows: {len(df)}")
                    except Exception as e:
                        st.error(f"Error reading file: {e}")
        
        with col2:
            st.subheader("Location Datasets")
            
            if not os.path.exists(NAMES_DATASETS_DIR):
                st.info("No location datasets found")
            else:
                location_datasets = glob.glob(os.path.join(NAMES_DATASETS_DIR, "*.csv"))
                
                if not location_datasets:
                    st.info("No location datasets found")
                else:
                    selected_location = st.selectbox(
                        "Select location dataset to view",
                        options=location_datasets,
                        format_func=os.path.basename
                    )
                    
                    if selected_location:
                        try:
                            df = pd.read_csv(selected_location)
                            st.dataframe(df.head(100))
                            st.info(f"Total rows: {len(df)}")
                        except Exception as e:
                            st.error(f"Error reading file: {e}")
        
        # Add a map visualization if a location dataset is selected
        if 'selected_location' in locals() and selected_location:
            st.subheader("Map Visualization")
            st.info("Displaying sample points on the map (first 100 geocoded points)")
            
            try:
                df = pd.read_csv(selected_location)
                
                # Filter only rows with valid location (not "Not geocoded" or errors)
                valid_locations = df[~df['location'].isin(['Not geocoded', 'Unknown location', 'Geocoding error'])]
                
                if valid_locations.empty:
                    st.warning("No points with valid location information found in the dataset")
                else:
                    # Get a sample for map display
                    map_sample = valid_locations.head(100)
                    
                    # We need lat/lon for the map, so we need to convert easting/northing
                    transformer = init_transformer()
                    
                    if transformer:
                        # Create new DataFrame with lat/lon for mapping
                        map_data = []
                        
                        for _, row in map_sample.iterrows():
                            lat, lon = convert_coordinates(row['easting'], row['northing'], transformer)
                            if lat and lon:
                                map_data.append({
                                    'lat': lat,
                                    'lon': lon,
                                    'location': row['location'],
                                    'point_id': row['point_id']
                                })
                        
                        if map_data:
                            map_df = pd.DataFrame(map_data)
                            st.map(map_df)
                        else:
                            st.warning("Could not convert coordinates for mapping")
                    else:
                        st.error("Coordinate transformer not available for map display")
            
            except Exception as e:
                st.error(f"Error creating map: {e}")
                import traceback
                st.error(traceback.format_exc())

if __name__ == "__main__":
    main() 