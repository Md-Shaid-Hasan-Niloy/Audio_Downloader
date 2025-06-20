import requests
import os
import yaml
import pandas as pd
import time
import json
import re

# Load credentials
credentials = yaml.safe_load(open('credentials.yaml'))
api = credentials['eBirdApiKey']

# Output folder
main_audio_folder = "audio_samples"
os.makedirs(main_audio_folder, exist_ok=True)

# Macaulay Library API
MACAULAY_URL = "https://search.macaulaylibrary.org/api/v1/search"
# Xeno-Canto API
XENO_CANTO_URL = "https://xeno-canto.org/api/2/recordings"

# Load species data
labels_url = 'https://raw.githubusercontent.com/mi3nts/mDashSupport/main/resources/birdCalls/labels.csv'
df_species = pd.read_csv(labels_url)

# Load taxonomy data
taxonomy_url = "https://raw.githubusercontent.com/mi3nts/mDashSupport/main/resources/birdCalls/eBird_taxonomy_codes_2021E.json"
taxonomy_data = requests.get(taxonomy_url).json()
df_taxonomy = pd.DataFrame.from_dict(taxonomy_data, orient='index', columns=['species'])
df_taxonomy.reset_index(inplace=True)
df_taxonomy.columns = ['code', 'species']

def sanitize(name):
    return re.sub(r'[\\/*?:"<>|]', '', name.replace(' ', '_'))

def get_taxon_code(scientific_name, common_name):
    species_key = f"{scientific_name}_{common_name}"
    try:
        return df_taxonomy[df_taxonomy['species'] == species_key]['code'].iloc[0]
    except IndexError:
        print(f"No taxon code found for: {common_name}")
        return None

def find_audio_asset_ids(taxon_code):
    params = {
        "taxonCode": taxon_code,
        "mediaType": "audio",
        "limit": 500
    }
    try:
        response = requests.get(MACAULAY_URL, params=params, timeout=10)
        response.raise_for_status()
        results = response.json().get('results', {}).get('content', [])
        return results
    except Exception as e:
        print(f"API error for taxon {taxon_code}: {str(e)}")
        return []

def download_audio(species_folder, filename, url, metadata):
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        filepath = os.path.join(species_folder, filename)
        jsonpath = filepath.rsplit(".", 1)[0] + ".json"

        if not os.path.exists(filepath):
            with open(filepath, 'wb') as f:
                f.write(response.content)
            with open(jsonpath, 'w') as f:
                json.dump(metadata, f, indent=2)
            print(f"✓ Saved: {filename} + metadata")
            return True
        else:
            print(f"Already exists: {filename}")
            return True
    except Exception as e:
        print(f"Download failed for {filename}: {str(e)}")
        return False

def fetch_xeno_canto(common_name, scientific_name, species_folder):
    print(f"🔎 Searching Xeno-Canto for: {scientific_name}")
    try:
        query = f"sp:{scientific_name.lower().replace(' ', '+')}"
        response = requests.get(f"{XENO_CANTO_URL}?query={query}", timeout=10)
        response.raise_for_status()
        recordings = response.json().get('recordings', [])
        for item in recordings:
            file_url = f"https:{item['file']}" if item['file'].startswith("//") else item['file']
            filename = f"{sanitize(common_name)}_XC_{item['id']}.mp3"
            metadata = {
                "source": "Xeno-Canto",
                "id": item['id'],
                "genus": item.get('gen'),
                "species": item.get('sp'),
                "location": item.get('loc'),
                "date": item.get('date'),
                "recordist": item.get('rec'),
                "country": item.get('cnt'),
                "license": item.get('lic'),
                "url": f"https://xeno-canto.org/{item['id']}"
            }
            if download_audio(species_folder, filename, file_url, metadata):
                time.sleep(1)
    except Exception as e:
        print(f"Xeno-Canto error for {scientific_name}: {e}")

def main():
    print("=====================================")
    print("  Macaulay + Xeno-Canto Downloader")
    print("=====================================")

    for _, row in df_species.iterrows():
        common_name = row['Common name']
        scientific_name = row['Scientific name']
        species_folder = os.path.join(main_audio_folder, sanitize(common_name))
        os.makedirs(species_folder, exist_ok=True)

        taxon_code = get_taxon_code(scientific_name, common_name)
        if taxon_code:
            macaulay_results = find_audio_asset_ids(taxon_code)
            for item in macaulay_results:
                asset_id = item.get("assetId")
                if not asset_id:
                    continue
                audio_url = f"https://cdn.download.ams.birds.cornell.edu/api/v1/asset/{asset_id}/audio"
                filename = f"{sanitize(common_name)}_ML_{asset_id}.mp3"
                metadata = {
                    "source": "Macaulay Library",
                    "assetId": asset_id,
                    "location": item.get("location"),
                    "date": item.get("date"),
                    "recordist": item.get("recordist"),
                    "country": item.get("country"),
                    "license": item.get("license"),
                    "url": f"https://macaulaylibrary.org/asset/{asset_id}"
                }
                download_audio(species_folder, filename, audio_url, metadata)
                time.sleep(1)

        fetch_xeno_canto(common_name, scientific_name, species_folder)

    print("\n✅ Download completed for all sources.")

if __name__ == "__main__":
    main()
