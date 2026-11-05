#!/usr/bin/env python3
"""
download_bts.py
Download BTS Reporting Carrier On-Time Performance data from BTS TranStats PREZIP bulk repository.
Supports downloading specific years and months, with resumption and progress bars.
"""

import os
import sys
import argparse
import zipfile
import urllib.request
from pathlib import Path
from tqdm import tqdm

BASE_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

def download_file(url: str, dest_path: Path):
    """Download a file with progress bar and custom User-Agent."""
    tmp_path = dest_path.with_name(dest_path.name + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"})
    with urllib.request.urlopen(req, timeout=30) as response:
        total_size = int(response.headers.get("Content-Length", 0))
        block_size = 1024 * 1024  # 1 MB

        with open(tmp_path, "wb") as f, tqdm(
            desc=dest_path.name,
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            while True:
                chunk = response.read(block_size)
                if not chunk:
                    break
                f.write(chunk)
                bar.update(len(chunk))

    received = tmp_path.stat().st_size
    if total_size and received != total_size:
        tmp_path.unlink()
        raise IOError(f"Incomplete download: got {received} of {total_size} bytes")
    tmp_path.replace(dest_path)

def extract_zip(zip_path: Path, extract_dir: Path):
    """Extract CSV from BTS zip file if not already extracted."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        for csv_file in csv_files:
            target_path = extract_dir / csv_file
            if target_path.exists() and target_path.stat().st_size > 1024 * 1024:
                print(f"[SKIP EXTRACT] {csv_file} already extracted ({target_path.stat().st_size / (1024*1024):.1f} MB)")
                continue
            print(f"Extracting {csv_file} ...")
            zf.extract(csv_file, extract_dir)

def main():
    parser = argparse.ArgumentParser(description="Download BTS On-Time Performance datasets.")
    parser.add_argument("--years", nargs="+", type=int, default=[2024], help="Years to download (default: 2024)")
    parser.add_argument("--months", nargs="+", type=int, default=list(range(1, 13)), help="Months to download (default: 1-12)")
    parser.add_argument("--output-dir", type=str, default="data/raw/bts", help="Directory to save downloads")
    parser.add_argument("--extract", action="store_true", help="Extract CSVs from zips after downloading")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Starting BTS Data Download ===")
    print(f"Years: {args.years}")
    print(f"Months: {args.months}")
    print(f"Destination: {out_dir.resolve()}\n")

    for year in args.years:
        for month in args.months:
            filename = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
            zip_dest = out_dir / filename
            url = BASE_URL.format(year=year, month=month)

            if zip_dest.exists() and zipfile.is_zipfile(zip_dest):
                print(f"[SKIP] {filename} already exists ({zip_dest.stat().st_size / (1024*1024):.1f} MB)")
            else:
                print(f"[DOWNLOADING] {url}")
                try:
                    download_file(url, zip_dest)
                except Exception as e:
                    print(f"[ERROR] Failed to download {url}: {e}", file=sys.stderr)
                    if zip_dest.exists():
                        zip_dest.unlink()
                    continue

            if args.extract:
                try:
                    extract_zip(zip_dest, out_dir)
                except Exception as e:
                    print(f"[ERROR] Failed to extract {filename}: {e}", file=sys.stderr)

    print("\n=== BTS Download Complete ===")

if __name__ == "__main__":
    main()
