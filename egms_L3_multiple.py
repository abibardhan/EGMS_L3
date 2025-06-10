import os
import zipfile
from io import BytesIO
import curl_cffi.requests as curl_requests
from time import sleep

# Configuration
BASE_URL = "https://egms.land.copernicus.eu/insar-api/archive/download/EGMS_L3_E{e}N{n}_100km_{d}_{year}_1.zip?id={id}"
DOWNLOAD_BASE = "Point_downloads"
ID = "7ce01544f73b4a9780b56f9c96fe4de3"
YEAR = "2018_2022" # 2018_2022, 2019_2023, 2020_2024
N_MIN = 27; N_MAX = 27
E_MIN = 33; E_MAX = 34
DISPLACEMENT_TYPES = ["U"]  # Options: "E" for East-West, "U" for Up-Down
DELAY = 5  # seconds between requests to avoid overwhelming the server

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
            sleep(10)
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
    print(f"Downloading data for E{E_MIN}-{E_MAX}, N{N_MIN}-{N_MAX}")
    
    total_tiles = (E_MAX - E_MIN + 1) * (N_MAX - N_MIN + 1) * len(DISPLACEMENT_TYPES)
    successful = 0
    failed = 0
    
    for e in range(E_MIN, E_MAX + 1):
        for n in range(N_MIN, N_MAX + 1):
            for d in DISPLACEMENT_TYPES:
                print(f"\nProcessing E{e}N{n} {d}...")
                
                success = download_tile(e, n, d)
                
                if success:
                    successful += 1
                    print(f"Successfully downloaded E{e}N{n} {d}")
                else:
                    failed += 1
                    print(f"Failed to download E{e}N{n} {d}")
                
                # Add delay between requests
                if not (e == E_MAX and n == N_MAX and d == DISPLACEMENT_TYPES[-1]):
                    print(f"Waiting {DELAY} seconds before next request...")
                    sleep(DELAY)
    
    print(f"\n=== Download Summary ===")
    print(f"Total tiles: {total_tiles}")
    print(f"Successfully downloaded: {successful}")
    print(f"Failed: {failed}") 