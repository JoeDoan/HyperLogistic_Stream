#!/usr/bin/env python3
"""
download_noaa.py
Download NOAA Integrated Surface Database (ISD) hourly weather data from AWS S3.
Maps US airport ICAO codes (e.g., KORD, KMEM, KATL) to NOAA USAF-WBAN stations via isd-history.csv.
"""

import os
import sys
import csv
import argparse
import urllib.request
from pathlib import Path
from tqdm import tqdm

ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
NOAA_S3_URL = "https://noaa-global-hourly-pds.s3.amazonaws.com/{year}/{station_id}.csv"

# Top 40 US logistics & air passenger hubs
DEFAULT_AIRPORTS = [
    # Top cargo & express hubs
    "KMEM",  # Memphis (FedEx SuperHub)
    "KSDF",  # Louisville (UPS Worldport)
    "KCVG",  # Cincinnati/Northern Kentucky (Amazon Air Hub & DHL)
    "KIND",  # Indianapolis (FedEx secondary hub)
    "KONT",  # Ontario, CA (major West coast freight hub)
    # Major passenger / multi-modal hubs
    "KATL", "KORD", "KDFW", "KDEN", "KLAX", "KJFK", "KEWR", "KSFO",
    "KCLT", "KLAS", "KPHX", "KMIA", "KIAH", "KBOS", "KSEA", "KMSP",
    "KDTW", "KPHL", "KBWI", "KSLC", "KDCA", "KIAD", "KSAN", "KTPA",
    "KBNA", "KAUS", "KMCO", "KRDU", "KSMF", "KMKE", "KPIT", "KCLE",
    "KSTL", "KMCI", "KPDX"  # KMCI is Kansas City
]

def download_file(url: str, dest_path: Path):
    """Download a file with progress bar."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        total_size = int(response.headers.get("Content-Length", 0))
        block_size = 1024 * 512

        with open(dest_path, "wb") as f, tqdm(
            desc=dest_path.name,
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            leave=False
        ) as bar:
            while True:
                chunk = response.read(block_size)
                if not chunk:
                    break
                f.write(chunk)
                bar.update(len(chunk))

def get_station_mapping(history_csv_path: Path, target_icaos: set):
    """Parse isd-history.csv and return mapping: icao -> (usaf, wban, name)."""
    mapping = {}
    with open(history_csv_path, mode="r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = next(reader)
        # Columns: USAF, WBAN, STATION NAME, CTRY, STATE, ICAO, LAT, LON, ELEV, BEGIN, END
        for row in reader:
            if len(row) > 10:
                icao = row[5].strip().upper()
                if icao in target_icaos:
                    usaf = row[0].strip()
                    wban = row[1].strip()
                    name = row[2].strip()
                    end_date = row[10].strip()
                    # Only pick currently active stations (end date >= 2024)
                    if end_date >= "20240101":
                        mapping[icao] = (usaf, wban, name)
    return mapping

def main():
    parser = argparse.ArgumentParser(description="Download NOAA ISD hourly weather data for US airport hubs.")
    parser.add_argument("--years", nargs="+", type=int, default=[2024], help="Years to download (default: 2024)")
    parser.add_argument("--airports", nargs="+", type=str, default=DEFAULT_AIRPORTS, help="List of airport ICAO codes")
    parser.add_argument("--output-dir", type=str, default="data/raw/noaa", help="Directory to save weather data")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Download or load isd-history.csv
    history_file = out_dir / "isd-history.csv"
    if not history_file.exists() or history_file.stat().st_size < 1000:
        print("[DOWNLOADING] NOAA station history inventory (isd-history.csv)...")
        download_file(ISD_HISTORY_URL, history_file)
    else:
        print(f"[FOUND] {history_file.name} already exists.")

    # 2. Map target airports to station IDs
    target_set = {ap.strip().upper() for ap in args.airports}
    mapping = get_station_mapping(history_file, target_set)
    print(f"Mapped {len(mapping)}/{len(target_set)} target airports to active NOAA stations.")

    # 3. Download hourly weather files for each year
    for year in args.years:
        year_dir = out_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n=== Downloading NOAA Weather for Year {year} ===")

        success_count = 0
        for icao, (usaf, wban, name) in sorted(mapping.items()):
            station_id = f"{usaf}{wban}"
            dest_file = year_dir / f"{icao}_{station_id}.csv"
            url = NOAA_S3_URL.format(year=year, station_id=station_id)

            if dest_file.exists() and dest_file.stat().st_size > 1024 * 10:
                print(f"[SKIP] {icao} ({name}): already downloaded ({dest_file.stat().st_size / (1024*1024):.2f} MB)")
                success_count += 1
                continue

            try:
                download_file(url, dest_file)
                print(f"[OK] {icao} ({name}) -> {dest_file.name} ({dest_file.stat().st_size / (1024*1024):.2f} MB)")
                success_count += 1
            except Exception as e:
                print(f"[WARNING] Could not download weather for {icao} ({station_id}): {e}")
                if dest_file.exists():
                    dest_file.unlink()

        print(f"Completed {success_count}/{len(mapping)} stations for {year}.")

    print("\n=== NOAA Download Finished ===")

if __name__ == "__main__":
    main()
