#!/usr/bin/env python3
"""
run_eda.py
Exploratory Data Analysis and Cleaning Pipeline for HyperLogistics-Stream.
Loads BTS flight data and NOAA hourly weather, applies type enforcement and UTC conversions,
performs stream-static join simulation, and outputs high-resolution analytical figures.
"""

import os
import re
import glob
from pathlib import Path
from datetime import datetime, timedelta
import zoneinfo

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set publication style
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['font.sans-serif'] = 'Helvetica, Arial, sans-serif'
plt.rcParams['axes.edgecolor'] = '#cccccc'
plt.rcParams['axes.linewidth'] = 0.8

# Airport timezone and ICAO lookup table
AIRPORT_METADATA = {
    'ATL': {'icao': 'KATL', 'tz': 'America/New_York', 'name': 'Atlanta Hartsfield'},
    'ORD': {'icao': 'KORD', 'tz': 'America/Chicago', 'name': "Chicago O'Hare"},
    'DFW': {'icao': 'KDFW', 'tz': 'America/Chicago', 'name': 'Dallas/Fort Worth'},
    'DEN': {'icao': 'KDEN', 'tz': 'America/Denver', 'name': 'Denver International'},
    'LAX': {'icao': 'KLAX', 'tz': 'America/Los_Angeles', 'name': 'Los Angeles International'},
    'JFK': {'icao': 'KJFK', 'tz': 'America/New_York', 'name': 'New York JFK'},
    'EWR': {'icao': 'KEWR', 'tz': 'America/New_York', 'name': 'Newark Liberty'},
    'SFO': {'icao': 'KSFO', 'tz': 'America/Los_Angeles', 'name': 'San Francisco International'},
    'MEM': {'icao': 'KMEM', 'tz': 'America/Chicago', 'name': 'Memphis (FedEx Hub)'},
    'SDF': {'icao': 'KSDF', 'tz': 'America/New_York', 'name': 'Louisville (UPS Hub)'},
    'CVG': {'icao': 'KCVG', 'tz': 'America/New_York', 'name': 'Cincinnati (Amazon Air Hub)'},
    'CLT': {'icao': 'KCLT', 'tz': 'America/New_York', 'name': 'Charlotte Douglas'},
    'LAS': {'icao': 'KLAS', 'tz': 'America/Los_Angeles', 'name': 'Las Vegas'},
    'PHX': {'icao': 'KPHX', 'tz': 'America/Phoenix', 'name': 'Phoenix Sky Harbor'},
    'MIA': {'icao': 'KMIA', 'tz': 'America/New_York', 'name': 'Miami International'},
    'IAH': {'icao': 'KIAH', 'tz': 'America/Chicago', 'name': 'Houston George Bush'},
    'BOS': {'icao': 'KBOS', 'tz': 'America/New_York', 'name': 'Boston Logan'},
    'SEA': {'icao': 'KSEA', 'tz': 'America/Los_Angeles', 'name': 'Seattle-Tacoma'},
    'MSP': {'icao': 'KMSP', 'tz': 'America/Chicago', 'name': 'Minneapolis-Saint Paul'},
    'DTW': {'icao': 'KDTW', 'tz': 'America/New_York', 'name': 'Detroit Metro'},
    'PHL': {'icao': 'KPHL', 'tz': 'America/New_York', 'name': 'Philadelphia International'},
    'BWI': {'icao': 'KBWI', 'tz': 'America/New_York', 'name': 'Baltimore/Washington'},
    'SLC': {'icao': 'KSLC', 'tz': 'America/Denver', 'name': 'Salt Lake City'},
    'MCO': {'icao': 'KMCO', 'tz': 'America/New_York', 'name': 'Orlando International'},
}

def parse_hhmm_to_time(val):
    """Convert float/int HHMM (e.g., 851.0 or 2400.0) into (hour, minute)."""
    if pd.isna(val):
        return None
    val_int = int(val)
    if val_int == 2400:
        return (0, 0, 1)  # rollover to next day
    hour = val_int // 100
    minute = val_int % 100
    if hour > 23 or minute > 59:
        return None
    return (hour, minute, 0)

def clean_bts_data(bts_csv_path: str, max_rows: int = 150000) -> pd.DataFrame:
    """Clean and standardize raw BTS flight data."""
    print(f"[BTS] Loading raw flights from {bts_csv_path} (limit={max_rows:,} rows)...")
    cols = [
        'FlightDate', 'Reporting_Airline', 'Tail_Number', 'Origin', 'Dest',
        'CRSDepTime', 'DepTime', 'DepDelay', 'WheelsOff', 'WheelsOn',
        'CRSArrTime', 'ArrTime', 'ArrDelay', 'Cancelled', 'Diverted', 'Distance'
    ]
    df = pd.read_csv(bts_csv_path, usecols=cols, nrows=max_rows, low_memory=False)
    initial_count = len(df)

    # 1. Filter cancelled and diverted flights
    df = df[(df['Cancelled'] == 0) & (df['Diverted'] == 0)].copy()
    df = df.dropna(subset=['DepDelay', 'ArrDelay', 'WheelsOff', 'CRSDepTime', 'CRSArrTime'])
    
    # 2. Filter for major hubs present in airport metadata
    df = df[df['Dest'].isin(AIRPORT_METADATA.keys()) & df['Origin'].isin(AIRPORT_METADATA.keys())].copy()
    print(f"[BTS] Filtered from {initial_count:,} to {len(df):,} non-cancelled hub-to-hub flights.")

    # 3. Compute target label: delta_delay
    df['delta_delay'] = df['ArrDelay'] - df['DepDelay']

    # 4. Resolve timezones and construct UTC departure & arrival timestamps
    utc_sched_dep = []
    utc_wheels_off = []
    utc_sched_arr = []
    sched_arr_hour_utc = []

    for _, row in df.iterrows():
        fdate_str = str(row['FlightDate'])
        fdate = datetime.strptime(fdate_str, '%Y-%m-%d')
        
        # Origin timezone for CRSDepTime and WheelsOff
        orig_tz_name = AIRPORT_METADATA[row['Origin']]['tz']
        orig_tz = zoneinfo.ZoneInfo(orig_tz_name)
        
        dep_parsed = parse_hhmm_to_time(row['CRSDepTime'])
        if dep_parsed:
            dep_dt = fdate.replace(hour=dep_parsed[0], minute=dep_parsed[1]) + timedelta(days=dep_parsed[2])
            dep_dt_aware = dep_dt.replace(tzinfo=orig_tz)
            crs_dep_utc = dep_dt_aware.astimezone(zoneinfo.ZoneInfo('UTC'))
        else:
            crs_dep_utc = None

        wo_parsed = parse_hhmm_to_time(row['WheelsOff'])
        if wo_parsed:
            wo_dt = fdate.replace(hour=wo_parsed[0], minute=wo_parsed[1]) + timedelta(days=wo_parsed[2])
            wo_dt_aware = wo_dt.replace(tzinfo=orig_tz)
            wo_utc = wo_dt_aware.astimezone(zoneinfo.ZoneInfo('UTC'))
            if crs_dep_utc is not None and wo_utc < (crs_dep_utc - timedelta(hours=1)):
                wo_utc = wo_utc + timedelta(days=1)
        else:
            wo_utc = None

        # Dest timezone for CRSArrTime
        dest_tz_name = AIRPORT_METADATA[row['Dest']]['tz']
        dest_tz = zoneinfo.ZoneInfo(dest_tz_name)
        
        arr_parsed = parse_hhmm_to_time(row['CRSArrTime'])
        if arr_parsed:
            arr_dt = fdate.replace(hour=arr_parsed[0], minute=arr_parsed[1]) + timedelta(days=arr_parsed[2])
            arr_dt_aware = arr_dt.replace(tzinfo=dest_tz)
            arr_utc = arr_dt_aware.astimezone(zoneinfo.ZoneInfo('UTC'))
            if crs_dep_utc is not None and arr_utc < crs_dep_utc:
                arr_utc = arr_utc + timedelta(days=1)
            arr_hour_utc = arr_utc.replace(minute=0, second=0, microsecond=0)
        else:
            arr_utc = None
            arr_hour_utc = None

        utc_sched_dep.append(crs_dep_utc)
        utc_wheels_off.append(wo_utc)
        utc_sched_arr.append(arr_utc)
        sched_arr_hour_utc.append(arr_hour_utc)

    df['sched_dep_utc'] = utc_sched_dep
    df['wheels_off_utc'] = utc_wheels_off
    df['sched_arr_utc'] = utc_sched_arr
    df['sched_arr_hour_utc'] = sched_arr_hour_utc
    df['dest_icao'] = df['Dest'].map(lambda d: AIRPORT_METADATA[d]['icao'])
    df['origin_icao'] = df['Origin'].map(lambda o: AIRPORT_METADATA[o]['icao'])

    df = df.dropna(subset=['sched_dep_utc', 'wheels_off_utc', 'sched_arr_hour_utc'])
    print(f"[BTS] Cleaned flights ready: {len(df):,} valid records.")
    return df

def clean_noaa_weather(noaa_dir: str) -> pd.DataFrame:
    """Parse, clean, and deduplicate NOAA hourly weather CSVs across available stations."""
    print(f"[NOAA] Loading weather files from {noaa_dir} ...")
    csv_files = glob.glob(os.path.join(noaa_dir, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No NOAA CSV files found in {noaa_dir}")

    records = []
    for filepath in csv_files:
        filename = Path(filepath).name
        match = re.match(r"^([A-Z0-9]{4})_(\d+)\.csv$", filename)
        if not match:
            continue
        icao = match.group(1)
        station_id = match.group(2)

        df = pd.read_csv(filepath, usecols=['DATE', 'WND', 'VIS', 'TMP', 'DEW'], low_memory=False)

        for _, row in df.iterrows():
            date_str = str(row['DATE'])
            try:
                dt_utc = datetime.fromisoformat(date_str).replace(tzinfo=zoneinfo.ZoneInfo('UTC'))
            except Exception:
                continue

            # Parse Wind: direction, quality, type, speed (m/s * 10), quality
            wind_speed = np.nan
            wind_dir = np.nan
            wnd_str = str(row['WND'])
            wnd_parts = wnd_str.split(',')
            if len(wnd_parts) >= 4:
                try:
                    wdir = int(wnd_parts[0])
                    wspeed = int(wnd_parts[3])
                    if wdir != 999:
                        wind_dir = wdir
                    if wspeed != 9999:
                        wind_speed = wspeed / 10.0  # convert to meters/sec
                except ValueError:
                    pass

            # Parse Visibility: distance (m), quality, variability, quality
            visibility = np.nan
            vis_str = str(row['VIS'])
            vis_parts = vis_str.split(',')
            if len(vis_parts) >= 1:
                try:
                    vdist = int(vis_parts[0])
                    if vdist != 999999:
                        visibility = vdist  # in meters
                except ValueError:
                    pass

            # Parse Temperature: signed celsius * 10, quality
            temp_c = np.nan
            tmp_str = str(row['TMP'])
            tmp_parts = tmp_str.split(',')
            if len(tmp_parts) >= 1:
                try:
                    tval = int(tmp_parts[0])
                    if tval != 9999:
                        temp_c = tval / 10.0
                except ValueError:
                    pass

            # Round to top of hour
            hour_utc = dt_utc.replace(minute=0, second=0, microsecond=0)

            records.append({
                'icao': icao,
                'station_id': station_id,
                'observed_at': dt_utc,
                'utc_hour': hour_utc,
                'wind_speed_mps': wind_speed,
                'wind_dir_deg': wind_dir,
                'visibility_m': visibility,
                'temp_c': temp_c,
            })

    weather_df = pd.DataFrame(records)
    print(f"[NOAA] Parsed {len(weather_df):,} raw weather observations across {len(csv_files)} stations.")

    # Deduplicate: Pick reading nearest to top of hour
    weather_df['minute_offset'] = (weather_df['observed_at'] - weather_df['utc_hour']).abs().dt.total_seconds()
    weather_df = weather_df.sort_values(['icao', 'utc_hour', 'minute_offset'])
    weather_clean = weather_df.drop_duplicates(subset=['icao', 'utc_hour'], keep='first').copy()
    weather_clean = weather_clean.drop(columns=['minute_offset'])

    print(f"[NOAA] Deduplicated to {len(weather_clean):,} hourly station readings.")
    return weather_clean

def perform_stream_join(flights_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """Simulate stream-to-static join between flight arrivals and hourly weather."""
    print("[JOIN] Joining flights with destination weather on (dest_icao, sched_arr_hour_utc) ...")
    
    # Ensure matching types for merge
    flights_df['join_hour'] = pd.to_datetime(flights_df['sched_arr_hour_utc'], utc=True)
    weather_df['join_hour'] = pd.to_datetime(weather_df['utc_hour'], utc=True)

    joined = pd.merge(
        flights_df,
        weather_df[['icao', 'join_hour', 'wind_speed_mps', 'visibility_m', 'temp_c']],
        left_on=['dest_icao', 'join_hour'],
        right_on=['icao', 'join_hour'],
        how='inner'
    )
    print(f"[JOIN] Successfully joined {len(joined):,} flights with live destination weather.")
    return joined

def generate_visualizations(joined_df: pd.DataFrame, output_dir: str):
    """Generate publication-ready diagnostic and exploratory plots."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    print(f"[VIZ] Generating figures in {out_path.resolve()} ...")

    # Filter out extreme outliers (> 6 hours) for clean visualization
    clean_sample = joined_df[(joined_df['DepDelay'] >= -30) & (joined_df['DepDelay'] <= 240) &
                             (joined_df['ArrDelay'] >= -45) & (joined_df['ArrDelay'] <= 300)].copy()

    # -------------------------------------------------------------
    # Figure 1: Delay Distribution Analysis
    # -------------------------------------------------------------
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    
    # 1A: DepDelay
    sns.histplot(clean_sample['DepDelay'], bins=40, kde=True, ax=axes[0], color='#1d4ed8')
    axes[0].set_title('Departure Delay Distribution', fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Departure Delay (minutes)')
    axes[0].set_ylabel('Flight Count')
    axes[0].axvline(0, color='red', linestyle='--', linewidth=1, label='On-time Threshold')
    axes[0].legend()

    # 1B: ArrDelay
    sns.histplot(clean_sample['ArrDelay'], bins=40, kde=True, ax=axes[1], color='#059669')
    axes[1].set_title('Arrival Delay Distribution', fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Arrival Delay (minutes)')
    axes[1].set_ylabel('Flight Count')
    axes[1].axvline(0, color='red', linestyle='--', linewidth=1, label='On-time Threshold')
    axes[1].legend()

    # 1C: Delta Delay (ArrDelay - DepDelay)
    sns.histplot(clean_sample['delta_delay'], bins=40, kde=True, ax=axes[2], color='#d97706')
    axes[2].set_title('Delta Delay: Target Label (Arr - Dep)', fontsize=12, fontweight='bold')
    axes[2].set_xlabel('Delta Delay (minutes)')
    axes[2].set_ylabel('Flight Count')
    axes[2].axvline(0, color='black', linestyle=':', linewidth=1.2, label='No Change Baseline')
    axes[2].legend()

    plt.tight_layout()
    fig1_file = out_path / 'fig1_delay_distributions.png'
    plt.savefig(fig1_file, dpi=300)
    plt.close()
    print(f"[VIZ] Saved {fig1_file.name}")

    # -------------------------------------------------------------
    # Figure 2: Destination Wind Speed vs. Delta Delay
    # -------------------------------------------------------------
    valid_weather = clean_sample.dropna(subset=['wind_speed_mps', 'delta_delay']).copy()
    valid_weather['wind_speed_knots'] = valid_weather['wind_speed_mps'] * 1.94384
    valid_weather['wind_bin'] = pd.cut(valid_weather['wind_speed_knots'], bins=[0, 5, 10, 15, 20, 25, 40],
                                       labels=['0-5 kt', '5-10 kt', '10-15 kt', '15-20 kt', '20-25 kt', '>25 kt'])

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.boxplot(x='wind_bin', y='delta_delay', data=valid_weather, ax=ax, palette='Blues', hue='wind_bin', legend=False, showfliers=False)
    ax.set_title('Destination Wind Speed vs. Airborne Delay Progression (Delta Delay)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Destination Surface Wind Speed (Knots)')
    ax.set_ylabel('Delta Delay (ArrDelay - DepDelay in minutes)')
    ax.axhline(0, color='red', linestyle='--', linewidth=1, label='Zero Change')
    ax.legend()
    plt.tight_layout()
    fig2_file = out_path / 'fig2_wind_speed_vs_delay.png'
    plt.savefig(fig2_file, dpi=300)
    plt.close()
    print(f"[VIZ] Saved {fig2_file.name}")

    # -------------------------------------------------------------
    # Figure 3: Visibility Conditions Impact (Dual Panel)
    # -------------------------------------------------------------
    vis_sample = clean_sample.dropna(subset=['visibility_m']).copy()
    vis_sample['visibility_miles'] = vis_sample['visibility_m'] / 1609.34
    vis_sample['vis_category'] = pd.cut(
        vis_sample['visibility_miles'],
        bins=[-0.1, 3.0, 6.0, 9.9, 100.0],
        labels=['Severe Fog (<3 mi)', 'Low Vis (3-6 mi)', 'Moderate (6-10 mi)', 'Clear (>=10 mi)']
    )

    summary = vis_sample.groupby('vis_category', observed=False).agg(
        mean_ArrDelay=('ArrDelay', 'mean')
    ).reset_index()

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    palette = ['#dc2626', '#f97316', '#3b82f6', '#10b981']

    # Subplot 3A: Mean Arrival Delay
    sns.barplot(x='vis_category', y='mean_ArrDelay', data=summary, ax=axes[0], palette=palette, hue='vis_category', legend=False)
    axes[0].set_title('Average Arrival Delay by Visibility Range', fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Destination Surface Visibility')
    axes[0].set_ylabel('Mean Arrival Delay (minutes)')
    for i, v in enumerate(summary['mean_ArrDelay']):
        axes[0].text(i, v + 0.3, f'{v:.1f} m', ha='center', fontweight='bold', fontsize=11)

    # Subplot 3B: Delta Delay Progression Boxplot
    sns.boxplot(x='vis_category', y='delta_delay', data=vis_sample, ax=axes[1], palette=palette, hue='vis_category', showfliers=False)
    axes[1].set_title('Airborne Delay Progression (Delta Delay) by Visibility', fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Destination Surface Visibility')
    axes[1].set_ylabel('Delta Delay (ArrDelay - DepDelay in minutes)')
    axes[1].axhline(0, color='black', linestyle='--', linewidth=1, label='Zero Delay Growth')
    axes[1].legend(loc='upper right')

    plt.tight_layout()
    fig3_file = out_path / 'fig3_visibility_vs_delay.png'
    plt.savefig(fig3_file, dpi=300)
    plt.close()
    print(f"[VIZ] Saved {fig3_file.name}")

    # -------------------------------------------------------------
    # Figure 4: Diurnal Congestion & Hourly Delay Pattern
    # -------------------------------------------------------------
    clean_sample['dep_hour'] = clean_sample['sched_dep_utc'].dt.hour
    hourly_stats = clean_sample.groupby('dep_hour').agg(
        flight_count=('FlightDate', 'count'),
        avg_dep_delay=('DepDelay', 'mean'),
        avg_arr_delay=('ArrDelay', 'mean')
    ).reset_index()

    fig, ax1 = plt.subplots(figsize=(10, 5))
    color = '#2563eb'
    ax1.set_xlabel('Departure Hour (UTC)', fontweight='bold')
    ax1.set_ylabel('Flight Volume', color=color, fontweight='bold')
    bars = ax1.bar(hourly_stats['dep_hour'], hourly_stats['flight_count'], color=color, alpha=0.35, width=0.7, label='Flight Volume')
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()
    color2 = '#dc2626'
    ax2.set_ylabel('Average Delay (minutes)', color=color2, fontweight='bold')
    line = ax2.plot(hourly_stats['dep_hour'], hourly_stats['avg_dep_delay'], color=color2, marker='o', linewidth=2.5, label='Avg Departure Delay')
    ax2.tick_params(axis='y', labelcolor=color2)

    plt.title('Diurnal Congestion Curve: Flight Volume vs. Average Departure Delay (UTC)', fontsize=12, fontweight='bold')
    fig.tight_layout()
    fig4_file = out_path / 'fig4_hourly_departure_patterns.png'
    plt.savefig(fig4_file, dpi=300)
    plt.close()
    print(f"[VIZ] Saved {fig4_file.name}")

    # -------------------------------------------------------------
    # Figure 5: Major Logistics Hub Inbound Congestion Comparison
    # -------------------------------------------------------------
    top_hubs = ['ORD', 'DFW', 'ATL', 'DEN', 'LAX', 'JFK', 'MEM', 'CVG', 'SDF']
    hub_sample = clean_sample[clean_sample['Dest'].isin(top_hubs)].copy()
    hub_metrics = hub_sample.groupby('Dest').agg(
        inbound_volume=('FlightDate', 'count'),
        avg_arr_delay=('ArrDelay', 'mean'),
        pct_delayed=('ArrDelay', lambda x: (x > 15).mean() * 100)
    ).reindex(top_hubs).reset_index()

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    sns.barplot(x='Dest', y='inbound_volume', data=hub_metrics, ax=axes[0], palette='crest', hue='Dest', legend=False)
    axes[0].set_title('Inbound Flight Volume by Major Hub', fontsize=12, fontweight='bold')
    axes[0].set_xlabel('Destination Hub')
    axes[0].set_ylabel('Number of Inbound Flights')

    sns.barplot(x='Dest', y='pct_delayed', data=hub_metrics, ax=axes[1], palette='flare', hue='Dest', legend=False)
    axes[1].set_title('Percentage of Inbound Flights Delayed > 15 Minutes', fontsize=12, fontweight='bold')
    axes[1].set_xlabel('Destination Hub')
    axes[1].set_ylabel('Delayed Flights (%)')

    plt.tight_layout()
    fig5_file = out_path / 'fig5_hub_congestion_comparison.png'
    plt.savefig(fig5_file, dpi=300)
    plt.close()
    print(f"[VIZ] Saved {fig5_file.name}")

def main():
    print("=== HyperLogistics-Stream: EDA and Data Cleaning Pipeline ===")
    bts_csv = 'data/raw/bts/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv'
    noaa_dir = 'data/raw/noaa/2024'
    reports_fig_dir = 'reports/figures'
    bronze_output = 'data/bronze/sample_cleaned_joined.parquet'

    # Step 1: Clean BTS Flights
    flights_clean = clean_bts_data(bts_csv, max_rows=150000)

    # Step 2: Clean NOAA Weather
    weather_clean = clean_noaa_weather(noaa_dir)

    # Step 3: Stream-Static Join
    joined_data = perform_stream_join(flights_clean, weather_clean)

    # Step 4: Export Cleaned Sample to Bronze Parquet
    print(f"[EXPORT] Saving joined sample to {bronze_output} ...")
    joined_data.to_parquet(bronze_output, engine='pyarrow', compression='snappy')
    print(f"[EXPORT] File saved successfully ({os.path.getsize(bronze_output)/(1024*1024):.2f} MB).")

    # Step 5: Generate Visualizations
    generate_visualizations(joined_data, reports_fig_dir)

    print("\n=== Data Pipeline Execution Complete ===")

if __name__ == '__main__':
    main()
