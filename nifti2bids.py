#!/usr/bin/env python3
"""
BIDSify dataset by reading imaging metadata from JSON sidecars.

Author: Tamjid Imtiaz
Description:
    - Iterates through subject folders.
    - Reads JSON sidecar files for sequence info.
    - Categorizes files (anat/func/dwi/fmap) based on metadata or filename.
    - Copies .nii.gz, .json, and diffusion sidecars (.bval, .bvec) to a BIDS-compliant structure.
    - If no mapping or NaN record_id, uses subject ID directly.
"""

import os
import json
import shutil
from pathlib import Path
import pandas as pd
import numpy as np


# CONFIGURATION

MAPPING_CSV = "..../subject_id_matching.csv" # a mapping file is a csv file which has the mapping between protocol defined subject id and a deidenfied subject ID. I used it to deidentify the subjects
SOURCE_DIR = Path("..../nifti") # This is the source directory containing the nifti files
BIDS_OUTPUT_DIR = Path("..../bids") # This is the output directory
SESSION_LABEL = "research" # name it based on your requirement


# LOAD MAPPING FILE

mapping_df = pd.read_csv(MAPPING_CSV)
mapping_df.columns = mapping_df.columns.str.strip().str.lower()

if not {"accession", "record_id"}.issubset(mapping_df.columns):
    raise ValueError("Mapping file must contain 'Accession' and 'Record_ID' columns.")

print(f"Loaded mapping file with {mapping_df.shape[0]} entries")


# HELPER FUNCTION: DETERMINE BIDS CATEGORY

def get_bids_category(json_info, filename):
    seqname = json_info.get("SequenceName", "")
    imagetype = "_".join(json_info.get("ImageType", []))
    fname = filename

    # Prioritize metadata and filename patterns. You can modify these criterias to add new modalities based on your json files.
    if "BOLD" in fname and "ORIGINAL" in imagetype and "DIS2D" in imagetype:
        return "func", "task-rest_bold"
    elif "AXIAL_T2" in fname and "SWI" not in fname and "ORIGINAL" in imagetype:
        return "anat", "acq-axial_T2w"
    elif "COR_T1_IR" in fname and "ORIGINAL" in imagetype:
        return "anat", "acq-hippocampalIR_T1w"
    elif "COR_T2" in fname and "ORIGINAL" in imagetype:
        return "anat", "acq-hippcor_T2w"
    elif "T2_FLAIR" in fname and "ORIGINAL" in imagetype:
        return "anat", "FLAIR"
    elif "SAG_T1_MPRAGE" in fname and "ORIGINAL" in imagetype:
        return "anat", "acq-mprage_T1w"
    elif "DTI" in fname and "TOPUP" in fname and "ORIGINAL" in imagetype:
        return "fmap", "acq-topup_dwi"
    elif "DTI" in fname and "ORIGINAL" in imagetype:
        return "dwi", "dwi"
    elif "DTI" in fname and "DERIVED" in imagetype and "ADC" in imagetype:
        return "dwi", "ADC"
    elif "DTI" in fname and "DERIVED" in imagetype and "TRACEW" in imagetype:
        return "dwi", "trace"
    elif "DTI" in fname and "DERIVED" in imagetype and "FA" in imagetype:
        return "dwi", "FA"
    else:
        return None, None


#  GOING THROUGH SUBJECT FOLDERS

subject_dirs = [d for d in SOURCE_DIR.iterdir() if d.is_dir()]
print(f"Found {len(subject_dirs)} subject directories.\n")

for subj_dir in subject_dirs:
    accession = subj_dir.name.strip()

    # --- record_id ---
    map_row = mapping_df[mapping_df["newaccession"].astype(str).str.strip() == accession]

    if map_row.empty:
        sub_label = f"sub-{accession}"
        print(f"No mapping found for accession {accession}. Using accession as ID.")
    else:
        record_id = map_row["record_id"].values[0]
        if pd.isna(record_id) or str(record_id).strip() == "":
            sub_label = f"sub-{accession}"
            print(f"NaN found for {accession}. Using accession as ID.")
        else:
            record_id_str = str(int(record_id)).zfill(4)  # pad to 4 digits
            sub_label = f"sub-RID{record_id_str}"

   
    # 4. FIND AND PROCESS NIFTI FILES
    
    nifti_files = list(subj_dir.rglob("*.nii.gz"))
    if not nifti_files:
        print(f"No NIfTI files found in {accession}")
        continue

    for nii_file in nifti_files:
        json_file = nii_file.with_suffix("").with_suffix(".json")  
        if not json_file.exists():
            print(f"JSON not found for {nii_file.name}")
            continue

        # Read JSON metadata
        try:
            with open(json_file, "r") as f:
                json_info = json.load(f)
        except Exception as e:
            print(f"Could not read {json_file.name}: {e}")
            continue

        category, bids_suffix = get_bids_category(json_info, nii_file.name)
        if category is None:
            print(f"Skipped {nii_file.name} (no matching rule)")
            continue

        # Prepare destination paths
        dest_dir = BIDS_OUTPUT_DIR / sub_label / f"ses-{SESSION_LABEL}" / category
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_base = dest_dir / f"{sub_label}_ses-{SESSION_LABEL}_{bids_suffix}"

        # --- Copy files ---
        dest_nii = dest_base.with_suffix(".nii.gz")
        dest_json = dest_base.with_suffix(".json")
        shutil.copy2(nii_file, dest_nii)
        shutil.copy2(json_file, dest_json)

        # --- Handle DWI sidecars ---
        if bids_suffix == "dwi":
            bval_file = nii_file.with_suffix("").with_suffix(".bval")
            bvec_file = nii_file.with_suffix("").with_suffix(".bvec")
            if bval_file.exists() and bvec_file.exists():
                shutil.copy2(bval_file, dest_base.with_suffix(".bval"))
                shutil.copy2(bvec_file, dest_base.with_suffix(".bvec"))

        print(f"Copied {nii_file.name} → {dest_nii.name}")

print("\n BIDSification complete!")
print(f"All files organized under: {BIDS_OUTPUT_DIR}")
