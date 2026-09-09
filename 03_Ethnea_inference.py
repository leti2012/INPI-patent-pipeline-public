import pandas as pd
import requests
import time
import urllib.parse
import re
import pickle
from pathlib import Path
from html import unescape
from tqdm import tqdm

# Configuration
INPUT_DIR = Path("dataset_inventors_unique/Output_Processed")
OUTPUT_DIR = Path("dataset_inventors_unique/Output_Ethnea")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ETHNEA_URL = "http://abel.lis.illinois.edu/cgi-bin/ethnea/search.py"
SLEEP_SECONDS = 0.5
CACHE_FILE = "local_ethnea_cache.pkl"

def load_cache():
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, "rb") as f:
            return pickle.load(f)
    return {}

def save_cache(cache_dict):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache_dict, f)

NAME_CACHE = load_cache()

def parse_ethnea_response(html):
    try:
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I)
        results = []
        for row in rows:
            cols = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.S | re.I)
            if not cols: continue
            clean = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", unescape(c))).strip() for c in cols]
            if len(clean) >= 6:
                eth, prob = clean[0], clean[1]
                if re.fullmatch(r"[A-Z\-]+", eth) and re.fullmatch(r"\d+(\.\d+)?", prob):
                    results.append({"ethnicity": eth, "prob": float(prob)})
        if not results: return None
        return max(results, key=lambda x: x['prob'])['ethnicity']
    except Exception:
        return None

def query_ethnea(first_name, last_name):
    if (first_name, last_name) in NAME_CACHE:
        return NAME_CACHE[(first_name, last_name)]
    first_enc = urllib.parse.quote(str(first_name))
    last_enc = urllib.parse.quote(str(last_name))
    url = f"{ETHNEA_URL}?Fname={first_enc}&Lname={last_enc}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            result = parse_ethnea_response(resp.text)
            NAME_CACHE[(first_name, last_name)] = result
            time.sleep(SLEEP_SECONDS)
            return result
    except Exception:
        pass
    return None

def process_file(file_path):
    out_path = OUTPUT_DIR / f"Ethnea_{file_path.name}"
    if out_path.exists():
        print(f"Skipping {file_path.name}, already processed.")
        return
    print(f"\nProcessing {file_path.name}...")
    df = pd.read_csv(file_path)
    unique_pairs = df[['first_name', 'last_name']].dropna().drop_duplicates()
    pairs_to_query = [
        (row['first_name'], row['last_name'])
        for _, row in unique_pairs.iterrows()
        if (row['first_name'], row['last_name']) not in NAME_CACHE
    ]

    if pairs_to_query:
        for i, (f_name, l_name) in enumerate(tqdm(pairs_to_query, desc="API Querying")):
            query_ethnea(f_name, l_name)
            if (i + 1) % 50 == 0:
                save_cache(NAME_CACHE)
        save_cache(NAME_CACHE)

    df['ethnea_pred'] = df.apply(
        lambda row: NAME_CACHE.get((row['first_name'], row['last_name']), None), axis=1
    )
    df.to_csv(out_path, index=False)
    print(f"Saved: {out_path.name}")

def main():
    input_files = sorted(INPUT_DIR.glob("ethnicity_inference_*.csv"))
    if not input_files:
        print(f"No CSV files found in {INPUT_DIR}.")
        return
    print(f"Found {len(input_files)} files. Starting local enrichment.")
    for f in input_files:
        process_file(f)
    print("\nProcessing complete.")

if __name__ == "__main__":
    main()
