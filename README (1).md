# INPI Patent Data & Inventor Ethnicity Pipeline

Production-ready ETL pipeline for processing French patent data from INPI (Institut National de la Propriété Industrielle) and predicting inventor ethnicities using NTEC and Ethnea models.

---

## Overview

**Inputs:** XML patent data (ST36 format) from INPI archives (1970–2025).

**Outputs:**
1. **Patents Dataset:** Yearly CSVs, one row per patent, including 5-year forward citations.
2. **Inventors Dataset:** Yearly CSVs, long format (one row per inventor), enriched with demographic and ethnicity predictions.

---

## Repository Scripts

| Script | Language | Description |
|---|---|---|
| `01_INPI_pipeline.R` | R | Core ETL pipeline. Extracts XML, consolidates data, filters for the French ecosystem, calculates forward citations, and exports results. |
| `02_Niggli_model_inference.py` | Python | Multiprocessing script. Downloads inventor data, deduplicates, and applies the NTEC TensorFlow model. |
| `03_Ethnea_inference.py` | Python | Local processing. Queries the Ethnea API for inventor names using a local cache. |
| `03.1_ethnea_in_background.py` | Python | Automated background script that queries the Ethnea API and syncs cache/processed CSVs. |

---

## Pipeline Architecture

### Stage 1: INPI Patent Processing (R)

| Phase | Description |
|---|---|
| **Phase 1: Extraction** | Download archives, iterate through nested archives, parse XML files (parallel batches), save incrementally. |
| **Phase 2: Consolidation** | Load all files, parse dates and filter by year. Apply French ecosystem filter and deduplicate by `(ID, kind)`. |
| **Phase 3: Forward Citations** | Explode references into edge list, normalize IDs (preserve country prefix), remove self-citations/duplicates, join with publication dates, count citations within a 5-year window. |
| **Phase 4: Export Patents** | Split by year and save CSVs (1970–2025). |
| **Phase 5: Export Inventors** | Align inventor names/locations and save long-format CSVs per year. |

### Stage 2: NTEC Ethnicity Prediction (Python)

- Explodes comma-separated inventors into distinct rows.
- Deduplicates by Patent ID and Individual Name.
- Uses a generated local worker script with `tf-keras` to run multiprocessing batch predictions on first and last names.
- Generates `ethnicity_enhanced` and `confidence_enhanced` scores based on rule-based combinations.

### Stage 3: Ethnea API Enrichment (Python)

- Scans NTEC output files for unique name pairs.
- Queries the Illinois Ethnea API for missing names.
- Maintains a persistent cache to respect API rate limits and ensure fault tolerance.

---

## Configuration

### Dependencies

**R:** `arrow`, `tidyverse`, `xml2`, `furrr`, `future`, `fs`, `jsonlite`, `lubridate`, `data.table`, `stringi`

**Python:** `pandas`, `numpy`, `tqdm`, `tf-keras`, `ntec`, `s3fs`, `requests`

### Storage

This pipeline is written for object storage (S3-compatible). Replace the placeholder bucket/paths in the config sections of each script with your own storage location before running.

### Performance Settings (R Pipeline)

- **Workers:** Auto-detected (max 20, min 1)
- **Batch size:** 100 files per checkpoint
- **Memory:** 32 GB recommended for full dataset

---

## Usage

### 1. Run the R Pipeline

    Rscript 01_INPI_pipeline.R

**Resume from checkpoint:** Pipeline automatically resumes from last checkpoint (`checkpoints_master/processed.json`). To restart from scratch:

    unlink("checkpoints_master/processed.json")

**Memory management:** If encountering RAM issues, reduce parallelism:

    CONFIG$n_workers <- 5
    CONFIG$batch_size <- 50

### 2. Run NTEC Predictions

    python 02_Niggli_model_inference.py

### 3. Run Ethnea Enrichment

    python 03.1_ethnea_in_background.py

> Monitor logs via `ethnea_s3_log.txt`

---

## Data Schemas

### Patents Dataset

| Column | Type | Description | Example |
|---|---|---|---|
| `id` | string | Publication number | FR2900308 |
| `kind` | string | Document type | A1, B1, A3, B3 |
| `status` | string | Publication status | PUBDEM, PUBDEL |
| `date_pub` | date | Publication date | 2008-10-24 |
| `date_app` | date | Application date | 2008-10-20 |
| `date_grant` | date | Grant date | 2009-05-15 |
| `date_priority` | string | Priority date(s) | 20061002; 20070315 |
| `priority_country` | string | Priority country(ies) | US; DE |
| `title` | string | Invention title | EQUIPEMENT ELECTRIQUE… |
| `firm_names` | string | Current rights holders | (example) |
| `firm_locations` | string | Firm addresses | Paris, FR; Toulouse, FR |
| `firm_ids` | string | SIREN numbers | 552059024 |
| `inventor_names` | string | Inventor names | (example) |
| `inventor_locations` | string | Inventor addresses | Paris, FR; Lyon, FR |
| `ipc_codes` | string | IPC classifications | H02J 7/00 20060101… |
| `cpc_codes` | string | CPC classifications | H02J 7/00; G06F 3/01 |
| `references_list` | string | Cited patents | FR1234567; EP9876543 |
| `pub_year` | integer | Publication year | 2008 |
| `citations_5y` | integer | Forward citations (5-year window) | 12 |

### Inventors Dataset (Enriched)

| Column | Type | Description |
|---|---|---|
| `id` | string | Patent publication number |
| `pub_year` | integer | Publication year |
| `individual_name` | string | Full individual inventor name |
| `first_name` | string | Parsed first name |
| `last_name` | string | Parsed last name |
| `inventor_location` | string | Inventor address |
| `first_name_ethnicity` | string | NTEC predicted ethnicity (first name) |
| `last_name_ethnicity` | string | NTEC predicted ethnicity (last name) |
| `ethnicity_enhanced` | string | Combined NTEC scoring |
| `confidence_enhanced` | float | NTEC model confidence |
| `ethnea_pred` | string | Ethnea API prediction |

---

## Key Features Explained

### 1. Kind Code Filter

Includes both applications **and** granted patents:

| Code | Description |
|---|---|
| A1 | Patent application (brevet) |
| A3 | Utility certificate application |
| B1 | Granted patent |
| B3 | Granted utility certificate |

Filter: `kind %in% c("A1", "A3", "B1", "B3")`

### 2. French Ecosystem Filter

Broad definition — includes patents with **any** French connection:
- Priority filed in France (`priority_country` contains FR)
- Applicant/owner located in France (`firm_locations` contains FR)
- Inventor located in France (`inventor_locations` contains FR)

Uses strict regex to avoid false matches (e.g., "San Francisco"): `\bFR\b|France`

### 3. Forward Citations Calculation

**Methodology:**
1. Extract all patent-to-patent citations from `<references-cited>`
2. Normalize IDs (uppercase + trim, preserving country prefix)
3. Remove self-citations (patent citing itself)
4. Remove duplicate citations
5. Join with publication dates
6. Filter citations within 5-year window: `0 <= time_lag <= 5 years`
7. Count citations per patent (`cited_id`)

> **Important:** Only counts citations where **both** citing and cited patents are in the INPI dataset (internal citations only).

### 4. Applicants vs. Owners

- **Applicants:** Original depositors at filing
- **Owners:** Current rights holders (may change via RNB inscriptions)

Pipeline prioritizes **owners** when available, and falls back to applicants otherwise.

### 5. Multiple Priorities

Supports patents with multiple priority claims (Paris Convention).

### 6. Safe XML Parsing

All XML text extraction calls are protected with `NA` checks to prevent silent data loss from missing XML nodes.

---

## Technical Implementation Details

### Checkpoint System
- Atomic writes using `.tmp` files + rename
- Tracks processed files in JSON format
- Enables resume capability without re-processing

### Memory Management
- Explicit garbage collection every 10 files
- Sequential parsing within parallel batches
- Incremental writes

### ID Normalization
- Preserves country prefix (FR, EP, US, etc.)
- Uppercase + trim to handle variations
- Avoids collisions (e.g., `FR123 ≠ EP123`)

### Deduplication Logic
- Groups by `(id, kind)`
- Scores records by information content
- Keeps the most complete record

### Date Handling
- Direct format specification: `"%Y%m%d"` (avoids ambiguity)
- Explicit `NA` handling for missing dates

---

## Data Quality Notes

1. **Date format:** INPI uses `YYYYMMDD` (no separators).
2. **Missing nodes:** Some old patents lack structured fields (pre-1992).
3. **IPC versions:** Classification codes include version indicators in the text field.
4. **Citations universe:** Forward citations limited to patents **in** the dataset. External citations (EP, US, WO) are excluded.
5. **Addresses:** Only available from 1992 onwards.
6. **SIREN numbers:** Only for French legal entities.
7. **CCP:** Supplementary Protection Certificates not included in the main pipeline.

---

## Note on Data & Privacy

This repository contains **code only** — no patent data, inventor names, or API cache files are included. Raw and processed data are stored separately in private object storage, not committed to this repository.
