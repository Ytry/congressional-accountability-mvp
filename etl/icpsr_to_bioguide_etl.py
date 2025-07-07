import yaml
import json
import requests
from io import StringIO

def load_yaml_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return yaml.safe_load(StringIO(response.text))

def extract_icpsr_to_bioguide(legislators):
    mapping = {}
    skipped = 0
    for leg in legislators:
        ids = leg.get("id", {})
        icpsr = ids.get("icpsr")
        bioguide = ids.get("bioguide")
        if icpsr and bioguide:
            mapping[str(icpsr)] = bioguide
        else:
            skipped += 1
    return mapping, skipped

def merge_mappings(*maps):
    merged = {}
    for m in maps:
        for k, v in m.items():
            if k in merged and merged[k] != v:
                print(f"Warning: Duplicate ICPSR ID with conflicting bioguide values: {k} => {merged[k]} vs {v}")
            merged[k] = v
    return merged

def main():
    current_url = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"
    historical_url = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-historical.yaml"
    output_path = "icpsr_to_bioguide_full.json"

    print("Downloading current legislator data...")
    current_data = load_yaml_from_url(current_url)

    print("Downloading historical legislator data...")
    historical_data = load_yaml_from_url(historical_url)

    current_map, current_skipped = extract_icpsr_to_bioguide(current_data)
    historical_map, historical_skipped = extract_icpsr_to_bioguide(historical_data)

    print(f"Extracted {len(current_map)} mappings from current (skipped {current_skipped})")
    print(f"Extracted {len(historical_map)} mappings from historical (skipped {historical_skipped})")

    full_map = merge_mappings(current_map, historical_map)

    with open(output_path, "w", encoding="utf-8") as out_file:
        json.dump(full_map, out_file, indent=2, sort_keys=True)

    print(f"✅ Saved full ICPSR → Bioguide mapping ({len(full_map)} entries) to '{output_path}'")

if __name__ == "__main__":
    main()
