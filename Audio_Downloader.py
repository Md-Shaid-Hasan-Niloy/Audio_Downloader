# ***************************************************************************Add commentMore actions
#   Animal Call Audio Downloader
#   ---------------------------------
#   Written by: Md Shaid Hasan Niloy
#   - for -
#   
#   - Written for -
#   Mints: Multi-scale Integrated Sensing and Simulation
#   ---------------------------------
#   Date: July 16, 2025

import requests
import os
import yaml
import pandas as pd
import time
import json
import re
import duckdb

# Load credentials
credentials = yaml.safe_load(open('credentials.yaml'))
api = credentials['eBirdApiKey']

# Output folder
main_audio_folder = "/mnt/mints/audio_samples"

os.makedirs(main_audio_folder, exist_ok=True)

# Macaulay Library API
MACAULAY_URL = "https://search.macaulaylibrary.org/api/v1/search"
# Xeno-Canto API
XENO_CANTO_URL = "https://xeno-canto.org/api/2/recordings"

# Load species list
labels_url = 'https://raw.githubusercontent.com/mi3nts/mDashSupport/main/resources/birdCalls/labels.csv'
df_species = pd.read_csv(labels_url)

# Set up DuckDB database
db_path = os.path.join(main_audio_folder, "audio_metadata.db")
con = duckdb.connect(db_path)
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

# Sanitize names for folder paths
def sanitize(name):
    return re.sub(r'[\\/*?:"<>|]', '', str(name).replace(' ', '_'))

# Taxonomy fetch from GBIF
def get_taxonomy_from_gbif(scientific_name):
    try:
        response = requests.get(f"https://api.gbif.org/v1/species/match?name={scientific_name}", timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            'class': data.get('class'),
            'order': data.get('order'),
            'family': data.get('family'),
        }
    except Exception as e:
        print(f"❌ Taxonomy fetch failed for {scientific_name}: {e}")
        return {'class': 'Unknown', 'order': 'Unknown', 'family': 'Unknown'}

# Save metadata to DuckDB
def store_metadata(meta):
    con.execute("""
        INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        meta.get('source'), meta.get('id'), meta.get('species'), meta.get('common_name'),
        meta.get('location'), meta.get('date'), meta.get('recordist'), meta.get('country'),
        meta.get('license'), meta.get('url'), meta.get('filename')
    ))

# Download audio + json metadata
def download_audio(species_folder, filename, url, metadata):
    try:
        filepath = os.path.join(species_folder, filename)
        jsonpath = filepath.rsplit(".", 1)[0] + ".json"
        if os.path.exists(filepath):
            print(f"↪️ Already exists: {filename}")
            return True

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        with open(filepath, 'wb') as f:
            f.write(response.content)
        with open(jsonpath, 'w') as f:
            json.dump(metadata, f, indent=2)
        store_metadata(metadata)
        print(f"✓ Saved: {filename}")
        return True
    except Exception as e:
        print(f"❌ Download failed for {filename}: {e}")
        return False

# Macaulay download using scientific name
def fetch_macaulay(scientific_name, common_name, species_folder):
    print(f"🔎 Searching Macaulay: {scientific_name}")
    base_params = {
        "query": scientific_name,
        "mediaType": "audio",
        
    }
    url = MACAULAY_URL
    while url:
        try:
            response = requests.get(url, params=base_params if url == MACAULAY_URL else {}, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            results = json_data.get('results', {}).get('content', [])
            for item in results:
                asset_id = item.get("assetId")
                if not asset_id:
                    continue
                audio_url = f"https://cdn.download.ams.birds.cornell.edu/api/v1/asset/{asset_id}/audio"
                filename = f"{sanitize(common_name)}_ML_{asset_id}.mp3"
                metadata = {
                    "source": "Macaulay Library",
                    "id": asset_id,
                    "species": item.get("scientificName"),
                    "common_name": common_name,
                    "location": item.get("location"),
                    "date": item.get("date"),
                    "recordist": item.get("recordist"),
                    "country": item.get("country"),
                    "license": item.get("license"),
                    "url": f"https://macaulaylibrary.org/asset/{asset_id}",
                    "filename": filename
                }
                download_audio(species_folder, filename, audio_url, metadata)
            
            # Go to next page
            url = json_data.get('results', {}).get('next')
        except Exception as e:
            print(f"❌ Macaulay error for {scientific_name}: {e}")
            break


# Xeno-Canto fetch
def fetch_xeno_canto(common_name, scientific_name, species_folder):
    print(f"🔎 Searching Xeno-Canto: {scientific_name}")
    try:
        params = {'query': scientific_name}
        response = requests.get(XENO_CANTO_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        num = data.get('numRecordings', len(data.get('recordings', [])))
        print(f"  → Found {num} recordings on Xeno‑Canto")
        
        for item in data.get('recordings', []):
            file_url = item['file']
            if file_url.startswith("//"):
                file_url = "https:" + file_url
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
            download_audio(species_folder, filename, file_url, metadata)
                
    except Exception as e:
        print(f"❌ Xeno‑Canto error for {scientific_name}: {e}")

# === MAIN ===
def main():
    print("=====================================")
    print("  Macaulay + Xeno-Canto Downloader")
    print("  Taxonomic Folder Hierarchy: Class > Order > Family > Species")
    print("=====================================\n")

    for _, row in df_species.iterrows():
        common_name = row['Common name']
        scientific_name = row['Scientific name']

        taxonomy = get_taxonomy_from_gbif(scientific_name)
        cls = sanitize(taxonomy['class'])
        order = sanitize(taxonomy['order'])
        family = sanitize(taxonomy['family'])
        species_folder = os.path.join(main_audio_folder, cls, order, family, sanitize(common_name))
        os.makedirs(species_folder, exist_ok=True)

        # Download from Macaulay using scientific name
        fetch_macaulay(scientific_name, common_name, species_folder)

        # Also try Xeno-Canto
        fetch_xeno_canto(common_name, scientific_name, species_folder)

    print("\n✅ Download complete. Metadata saved to DuckDB.")

if __name__ == "__main__":
    main()
