# ***************************************************************************Add commentMore actions
#   Animal Call Audio Downloader
#   ---------------------------------
#   Written by: Md Shaid Hasan Niloy
#   - for -
#   
#   - Written for -
#   Mints: Multi-scale Integrated Sensing and Simulation
#   ---------------------------------
#   Date: June 19, 2025
import requests
import os
import yaml
import pandas as pd
import time
import re
import duckdb
import json

# Load credentials
credentials = yaml.safe_load(open('credentials.yaml'))
api = credentials['eBirdApiKey']

# Output base folder
main_audio_folder = "audio_samples"
os.makedirs(main_audio_folder, exist_ok=True)

# APIs
MACAULAY_URL = "https://search.macaulaylibrary.org/api/v1/search"
XENO_CANTO_URL = "https://xeno-canto.org/api/2/recordings"

# Load species list with common & scientific names
labels_url = 'https://raw.githubusercontent.com/mi3nts/mDashSupport/main/resources/birdCalls/labels.csv'
df_species = pd.read_csv(labels_url)

# Load taxonomy data (make sure this CSV file is present in your folder)
df_full_taxonomy = pd.read_csv("eBird_taxonomy_v2024.csv", sep=",")
# Only keep needed columns
df_full_taxonomy = df_full_taxonomy[['PRIMARY_COM_NAME', 'SCI_NAME', 'ORDER', 'FAMILY', 'SPECIES_CODE']]

# Setup DuckDB database & table for metadata
con = duckdb.connect("audio_metadata.db")
con.execute("""
CREATE TABLE IF NOT EXISTS metadata (
    source TEXT,
    id TEXT,
    species TEXT,
    common_name TEXT,
    location TEXT,
    date TEXT,
    recordist TEXT,
    country TEXT,
    license TEXT,
    url TEXT,
    filename TEXT
)
""")

def sanitize(name):
    """Sanitize folder and filenames to remove problematic characters."""
    return re.sub(r'[\\/*?:"<>|]', '', name.replace(' ', '_'))

def get_taxonomy_info(common_name, scientific_name):
    row = df_full_taxonomy[
        (df_full_taxonomy['PRIMARY_COM_NAME'] == common_name) & 
        (df_full_taxonomy['SCI_NAME'] == scientific_name)
    ]
    if not row.empty:
        return row.iloc[0]['ORDER'], row.iloc[0]['FAMILY']
    else:
        print(f"Taxonomy info not found for {common_name} / {scientific_name}")
        return None, None

def get_taxon_code(scientific_name, common_name):
    row = df_full_taxonomy[
        (df_full_taxonomy['PRIMARY_COM_NAME'] == common_name) &
        (df_full_taxonomy['SCI_NAME'] == scientific_name)
    ]
    if not row.empty:
        return row.iloc[0]['SPECIES_CODE']  # Use SPECIES_CODE as taxonCode
    else:
        print(f"No taxon code found for {common_name} / {scientific_name}")
        return None

def find_audio_asset_ids(taxon_code, max_items=21):
    params = {
        "taxonCode": taxon_code,
        "mediaType": "audio",
        "limit": max_items
    }
    try:
        response = requests.get(MACAULAY_URL, params=params, timeout=10)
        response.raise_for_status()
        content = response.json().get('results', {}).get('content', [])
        return content[:max_items]
    except Exception as e:
        print(f"API error for taxon {taxon_code}: {str(e)}")
        return []

def store_metadata(meta):
    con.execute("""
        INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        meta.get('source'), meta.get('id'), meta.get('species'), meta.get('common_name'),
        meta.get('location'), meta.get('date'), meta.get('recordist'), meta.get('country'),
        meta.get('license'), meta.get('url'), meta.get('filename')
    ))

def download_audio(species_folder, filename, url, metadata):
    try:
        filepath = os.path.join(species_folder, filename)
        jsonpath = filepath.rsplit(".", 1)[0] + ".json"
        if os.path.exists(filepath):
            print(f"Already exists: {filename}")
            return False
        
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"✓ Saved: {filename}")

        # Save metadata JSON file alongside audio
        with open(jsonpath, 'w', encoding='utf-8') as jf:
            json.dump(metadata, jf, indent=2)

        # Store metadata in DuckDB
        store_metadata(metadata)
        return True
    except Exception as e:
        print(f"Download failed for {filename}: {str(e)}")
        return False

def fetch_xeno_canto(common_name, scientific_name, species_folder, max_items=21):
    print(f"🔎 Searching Xeno-Canto for: {scientific_name}")
    try:
        query = f"sp:{scientific_name.lower().replace(' ', '+')}"
        response = requests.get(f"{XENO_CANTO_URL}?query={query}", timeout=10)
        response.raise_for_status()
        recordings = response.json().get('recordings', [])[:max_items]
        for item in recordings:
            file_url = f"https:{item['file']}" if item['file'].startswith("//") else item['file']
            filename = f"{sanitize(common_name)}_XC_{item['id']}.mp3"
            metadata = {
                "source": "Xeno-Canto",
                "id": item['id'],
                "species": item.get('sp'),
                "common_name": common_name,
                "location": item.get('loc'),
                "date": item.get('date'),
                "recordist": item.get('rec'),
                "country": item.get('cnt'),
                "license": item.get('lic'),
                "url": f"https://xeno-canto.org/{item['id']}",
                "filename": filename
            }
            if download_audio(species_folder, filename, file_url, metadata):
                time.sleep(1)
    except Exception as e:
        print(f"Xeno-Canto error for {scientific_name}: {e}")

def main():
    print("=====================================")
    print("  Macaulay + Xeno-Canto Downloader")
    print("=====================================")

    for idx, row in df_species.iterrows():
        common_name = row['Common name']
        scientific_name = row['Scientific name']

        order, family = get_taxonomy_info(common_name, scientific_name)
        if order and family:
            folder_path = os.path.join(main_audio_folder, sanitize(order), sanitize(family), sanitize(common_name))
        else:
            folder_path = os.path.join(main_audio_folder, sanitize(common_name))

        os.makedirs(folder_path, exist_ok=True)

        taxon_code = get_taxon_code(scientific_name, common_name)
        if taxon_code:
            print(f"🔎 Searching Macaulay Library for taxon code: {taxon_code}")
            macaulay_results = find_audio_asset_ids(taxon_code, max_items=21)
            for item in macaulay_results:
                asset_id = item.get("assetId")
                if not asset_id:
                    continue
                audio_url = f"https://cdn.download.ams.birds.cornell.edu/api/v1/asset/{asset_id}/audio"
                filename = f"{sanitize(common_name)}_ML_{asset_id}.mp3"
                metadata = {
                    "source": "Macaulay Library",
                    "id": str(asset_id),
                    "species": item.get('scientificName'),
                    "common_name": common_name,
                    "location": item.get('location'),
                    "date": item.get('date'),
                    "recordist": item.get('recordist'),
                    "country": item.get('country'),
                    "license": item.get('license'),
                    "url": f"https://macaulaylibrary.org/asset/{asset_id}",
                    "filename": filename
                }
                download_audio(folder_path, filename, audio_url, metadata)
                time.sleep(1)
        else:
            print(f"No taxon code for {common_name}, skipping Macaulay.")

        # Always fetch from Xeno-Canto as fallback or supplement
        fetch_xeno_canto(common_name, scientific_name, folder_path, max_items=21)

    print("\n✅ Download completed and metadata stored in DuckDB.")

if __name__ == "__main__":
    main()
