"""
NTEC Ethnicity Prediction Pipeline
==================================
This script performs ethnicity inference on patent inventor data using the NTEC model.

Key features:
1. Downloads data from object storage.
2. Handles multi-inventor fields via explosion.
3. Removes duplicates to ensure unique (Patent ID + Inventor Name) pairs.
4. Uses multiprocessing for high-performance inference.
5. Predicts ethnicity for First Name, Last Name, and a combined 'Enhanced' score.
"""

import os
import sys
import time
import subprocess
import importlib
import pandas as pd
import numpy as np
from pathlib import Path
from multiprocessing import Pool, cpu_count, set_start_method
from tqdm import tqdm

# ============================================================================
# WORKER SCRIPT GENERATION
# (Required for Python multiprocessing to work correctly with TensorFlow)
# ============================================================================
worker_script = r'''
import pandas as pd
import numpy as np
import tf_keras
import ntec
import ntec.Classifiers
from functools import lru_cache
from pathlib import Path

# Global model instance
clf = None

def init_worker():
    """Initializes the NTEC model within the worker process."""
    global clf
    try:
        ntec.Classifiers.model_from_json = tf_keras.models.model_from_json
        if clf is None:
            clf = ntec.Classifier("joeg")
    except Exception:
        pass

@lru_cache(maxsize=50000)
def split_name(full_name):
    """Splits full name string into First and Last names."""
    if pd.isna(full_name) or not full_name: return '', ''
    parts = str(full_name).strip().split()
    if not parts: return '', ''
    if len(parts) == 1: return ('', parts[0]) if parts[0].isupper() else (parts[0], '')
    prefixes = {'BEN', 'ABU', 'ABOU', 'EL', 'AL', 'IBN', 'ABD'}
    parts_upper = [p.upper() for p in parts]
    idx = 1
    if parts_upper[0] in prefixes:
        i = 0
        while i < len(parts_upper) and parts_upper[i] in prefixes: i += 1
        idx = i + 1 if i < len(parts_upper) else len(parts)
    return ' '.join(parts[idx:]), ' '.join(parts[:idx])

def predict_batch_local(names):
    """Batch prediction logic."""
    if not names: return []
    res = [None] * len(names)
    valid_idxs, clean_names = [], []
    for i, n in enumerate(names):
        if pd.isna(n) or not str(n).strip():
            res[i] = (None, 0.0)
            continue
        try:
            c = ntec.clean_name(str(n))
            if c:
                valid_idxs.append(i)
                clean_names.append(c)
            else: res[i] = (None, 0.0)
        except: res[i] = (None, 0.0)
    if clean_names and clf:
        try:
            enc = [clf.encode_name(n) for n in clean_names]
            preds = clf.predict_origins(np.array(enc))
            for i, v_idx in enumerate(valid_idxs):
                p = preds.iloc[i, 1:].astype(float)
                res[v_idx] = (p.idxmax(), p.max())
        except: pass
    return res

def process_file(args):
    """Main worker function: Reads, Explodes, Predicts, Saves."""
    file_path, output_dir = Path(args[0]), Path(args[1])
    try:
        if "Output" in str(file_path): return 0
        df = pd.read_csv(file_path)
        year = file_path.stem.split('_')[-1]

        # Explode comma-separated inventors
        df['inventor_list'] = df['inventor_name'].fillna('').astype(str).str.split(',')
        df = df.explode('inventor_list')
        df['individual_name'] = df['inventor_list'].str.strip()
        df = df[df['individual_name'].str.len() > 1].copy()

        # Handle Duplicates: Drop if Patent ID AND Name are identical
        if 'id' in df.columns:
            df.drop_duplicates(subset=['id', 'individual_name'], inplace=True)
        else:
            df.drop_duplicates(subset=['individual_name'], inplace=True)

        # Name Splitting
        df['first_name'], df['last_name'] = zip(*[split_name(n) for n in df['individual_name']])

        # Predictions
        f_res = predict_batch_local(df['first_name'].tolist())
        l_res = predict_batch_local(df['last_name'].tolist())

        # Assign Results
        df['first_name_ethnicity'] = [fp for fp, fc in f_res]
        df['first_name_confidence'] = [fc for fp, fc in f_res]
        df['last_name_ethnicity'] = [lp for lp, lc in l_res]
        df['last_name_confidence'] = [lc for lp, lc in l_res]

        # Combine Results
        final_eth, final_conf = [], []
        for (fp, fc), (lp, lc) in zip(f_res, l_res):
            if fp is None: final_eth.append(lp); final_conf.append(lc)
            elif lp is None: final_eth.append(fp); final_conf.append(fc)
            elif fp == lp and min(fc, lc) > 0.7: final_eth.append(fp); final_conf.append((fc+lc)/2)
            elif fc > 0.85: final_eth.append(fp); final_conf.append(fc)
            elif lc > 0.85: final_eth.append(lp); final_conf.append(lc)
            else:
                if fc > lc: final_eth.append(fp); final_conf.append(fc)
                else: final_eth.append(lp); final_conf.append(lc)

        df['ethnicity_enhanced'] = final_eth
        df['confidence_enhanced'] = final_conf

        # Cleanup & Save
        if 'ethnicity' in df.columns:
            df.rename(columns={'ethnicity': 'eth_orig', 'confidence': 'conf_orig'}, inplace=True)
        df.drop(columns=['inventor_list'], inplace=True, errors='ignore')

        cols = [
            'id', 'individual_name', 'first_name', 'last_name',
            'first_name_ethnicity', 'first_name_confidence',
            'last_name_ethnicity', 'last_name_confidence',
            'ethnicity_enhanced', 'confidence_enhanced'
        ]
        existing = [c for c in df.columns if c not in cols]
        df = df[existing + [c for c in cols if c in df.columns]]

        df.to_csv(output_dir / f"ethnicity_inference_{year}.csv", index=False)
        return len(df)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0
'''

# Write the worker script to disk
with open("ntec_worker.py", "w", encoding="utf-8") as f:
    f.write(worker_script)

# ============================================================================
# MAIN PIPELINE EXECUTION
# ============================================================================
def main():
    # A. Setup
    print("Initializing environment...")
    pkgs = ['pandas', 'numpy', 'tqdm', 'tf-keras', 'ntec', 's3fs']
    for p in pkgs:
        try: __import__(p.replace('-', '_'))
        except ImportError: subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--break-system-packages', p])

    # Configuration
    # NOTE: replace with your own bucket/paths before running.
    BUCKET = "<your-bucket-name>"
    SOURCE_FOLDER = "RA INPI/FINAL VERSION 0202/dataset_inventors_unique"
    INPUT_DIR = Path("dataset_inventors_unique")
    OUTPUT_DIR = INPUT_DIR / "Output_Processed"
    FINAL_FILE = INPUT_DIR / "NTEC_Final_Results_Unique.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # B. Download Data
    import s3fs
    if not INPUT_DIR.exists() or not list(INPUT_DIR.rglob("*.csv")):
        print(f"Downloading data from object storage ({SOURCE_FOLDER})...")
        try:
            fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': 'https://minio.lab.sspcloud.fr'})
            fs.get(f"{BUCKET}/{SOURCE_FOLDER}", str(INPUT_DIR), recursive=True)
        except Exception as e:
            print(f"Download failed: {e}")
            return

    # C. Run Parallel Processing
    import ntec_worker
    importlib.reload(ntec_worker)
    try: set_start_method('spawn', force=True)
    except RuntimeError: pass

    files = sorted(INPUT_DIR.rglob('*.csv'))
    files = [f for f in files if "Output" not in str(f) and "Report" not in str(f)]
    if not files:
        print("No input files found.")
        return

    print(f"Processing {len(files)} files with duplicate handling...")
    tasks = [(str(f), str(OUTPUT_DIR)) for f in files]
    start = time.time()
    n_workers = max(1, cpu_count() - 4)

    with Pool(n_workers, initializer=ntec_worker.init_worker) as pool:
        counts = list(tqdm(pool.imap(ntec_worker.process_file, tasks), total=len(files)))

    print(f"Processing complete. {sum(counts):,} rows in {int(time.time()-start)}s")

    # D. Merge Results
    print("Merging results...")
    out_files = sorted(OUTPUT_DIR.glob("*.csv"))
    if not out_files: return

    df_list = []
    cols = [
        'id', 'individual_name', 'first_name', 'last_name',
        'first_name_ethnicity', 'first_name_confidence',
        'last_name_ethnicity', 'last_name_confidence',
        'ethnicity_enhanced', 'confidence_enhanced'
    ]
    for f in tqdm(out_files, desc="Merging"):
        try:
            df_curr = pd.read_csv(f, nrows=1)
            valid_cols = [c for c in cols if c in df_curr.columns]
            df_list.append(pd.read_csv(f, usecols=valid_cols))
        except: pass

    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        full_df.to_csv(FINAL_FILE, index=False)
        print(f"Saved final file: {FINAL_FILE}")

if __name__ == "__main__":
    main()
