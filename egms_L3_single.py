import os
import zipfile
from io import BytesIO
import curl_cffi.requests as curl_requests

# Configuration
BASE_URL = "https://egms.land.copernicus.eu/insar-api/archive/download/EGMS_L3_E{e}N{n}_100km_{d}_{year}_1.zip?id={id}"
DOWNLOAD_BASE = "Point_downloads"
ID = "7ce01544f73b4a9780b56f9c96fe4de3"
YEAR="2018_2022" # 2018_2022, 2019_2023, 2020_2024
N_COORD = 27; E_COORD = 40; DISPLACEMENT_TYPE = "U"  # Options: "E" for East-West, "U" for Up-Down

def download_tile(e, n, d):
    """Download a single tile with given coordinates and displacement type"""
    tile_code = f"E{e}N{n}"
    filename_prefix = f"EGMS_L3_{tile_code}_100km_{d}_{YEAR}_1"
    url = BASE_URL.format(e=e, n=n, d=d, year=YEAR, id=ID)
    
    try:
        response = curl_requests.get(url, timeout=600)  # 10 minutes timeout
        print(f"Response for {tile_code} {d}: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Failed to download {tile_code} {d}")
            return False
        
        # Create download directory if it doesn't exist
        os.makedirs(DOWNLOAD_BASE, exist_ok=True)
        
        # Read zip from memory
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            for name in z.namelist():
                if name.endswith(".csv") and filename_prefix in name:
                    z.extract(name, path=DOWNLOAD_BASE)
                    print(f"Extracted {name}")
                    return True
        
        print(f"No matching CSV found in the downloaded zip for {tile_code} {d}")
        return False
    
    except Exception as e:
        print(f"Error downloading {tile_code} {d}: {e}")
        return False

if __name__ == "__main__":
    print(f"=== EGMS L3 Download Tool ===")
    print(f"Downloading data for E{E_COORD}N{N_COORD} {DISPLACEMENT_TYPE}...")
    
    success = download_tile(E_COORD, N_COORD, DISPLACEMENT_TYPE)
    
    if success:
        print(f"Successfully downloaded E{E_COORD}N{N_COORD} {DISPLACEMENT_TYPE}")
    else:
        print(f"Failed to download E{E_COORD}N{N_COORD} {DISPLACEMENT_TYPE}") 