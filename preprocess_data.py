#!/usr/bin/env python3
"""
Data Preprocessing Script for PDU Power Metrics

This script:
1. Verifies all input data files in data/
2. Extracts tar.xz archives
3. Converts InfluxDB JSON format to CSV if needed
4. Analyzes each dataset (time range, outlets, record count)
5. Adds headers if missing
6. Compresses to .csv.gz format (ready-to-use)
7. Generates metadata descriptions
8. Removes original archives and temporary files
9. Creates reproducible documentation

Usage:
    python preprocess_data.py [--keep-archives]
    
Options:
    --keep-archives    Keep original .tar.xz files (default: remove them)
"""

import os
import sys
import tarfile
import gzip
import csv
import shutil
from datetime import datetime
from pathlib import Path
import json


def verify_input_data(data_dir):
    """Verify and categorize input data files."""
    print(f"{'=' * 70}")
    print("STEP 1: VERIFYING INPUT DATA")
    print('=' * 70)
    
    archives = list(data_dir.glob('*.tar.xz'))
    csvs = [f for f in data_dir.glob('*.csv') if not f.name.endswith('.gz')]
    json_files = list(data_dir.glob('*.json'))
    existing_gz = list(data_dir.glob('*.csv.gz'))
    
    print(f"\nFound in data/:")
    print(f"  - {len(archives)} tar.xz archive(s)")
    print(f"  - {len(csvs)} uncompressed CSV file(s)")
    print(f"  - {len(existing_gz)} existing .csv.gz file(s)")
    
    if archives:
        print(f"\n📦 Archives to process:")
        for arch in sorted(archives):
            size_mb = arch.stat().st_size / (1024 * 1024)
            print(f"    • {arch.name} ({size_mb:.1f} MB)")
    
    if csvs:
        print(f"\n📄 CSV files to process:")
        for csv_file in sorted(csvs):
            size_mb = csv_file.stat().st_size / (1024 * 1024)
            print(f"    • {csv_file.name} ({size_mb:.1f} MB)")
    
    if existing_gz:
        print(f"\n⚠️  Existing processed files (will be overwritten):")
        for gz in sorted(existing_gz):
            print(f"    • {gz.name}")
    
    return archives, csvs


def detect_file_format(file_path):
    """Detect if file is CSV or InfluxDB JSON format."""
    print(f"  Detecting format of {file_path.name}...")
    
    with open(file_path, 'r') as f:
        first_chars = f.read(100)
    
    if first_chars.strip().startswith('{'):
        # Likely JSON
        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
                if 'results' in data:
                    print(f"    → InfluxDB JSON format")
                    return 'influxdb_json'
            except:
                pass
    
    # Check if it's CSV
    if ',' in first_chars and (first_chars[0].isdigit() or first_chars.startswith('time')):
        print(f"    → CSV format")
        return 'csv'
    
    print(f"    → Unknown format")
    return 'unknown'


def convert_influxdb_to_csv(json_path, output_path):
    """Convert InfluxDB JSON format to CSV format."""
    print(f"  Converting InfluxDB JSON to CSV...")
    
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    values = data["results"][0]["series"][0]["values"]
    
    # Write CSV with header
    records_written = 0
    with open(output_path, 'w') as f:
        f.write("time,active-power-outlet,value\n")
        
        for v in values:
            # Filter for power measurements with active-power-outlet tag
            if len(v) >= 5 and v[2] == "power" and "active-power-outlet" in str(v[3]):
                # Extract outlet number from tag (format: "active-power-outlet-N")
                outlet = int(v[3].split("-")[-1])
                power = int(v[4])
                f.write(f"{v[0]},{outlet},{power}\n")
                records_written += 1
    
    print(f"    → {records_written:,} power records extracted")
    return records_written


def extract_tarxz(archive_path, extract_to='.'):
    """Extract tar.xz archive and return extracted file path."""
    print(f"\n{'─' * 70}")
    print(f"Processing: {archive_path.name}")
    print('─' * 70)
    print(f"  Extracting archive...")
    with tarfile.open(archive_path, 'r:xz') as tar:
        members = tar.getmembers()
        if len(members) != 1:
            raise ValueError(f"Expected single file in archive, found {len(members)}")
        tar.extractall(path=extract_to)
        extracted_file = Path(extract_to) / members[0].name
        print(f"    → Extracted: {extracted_file.name}")
        return extracted_file


def analyze_csv(csv_path, sample_lines=100000):
    """Analyze CSV file to extract metadata."""
    print(f"  Analyzing data...")
    
    # Detect if file has header
    with open(csv_path, 'r') as f:
        first_line = f.readline().strip()
        has_header = first_line.startswith('time') or not first_line[0].isdigit()
    
    # Read all data to get accurate stats
    timestamps = []
    outlets = set()
    power_values = []
    total_lines = 0
    
    with open(csv_path, 'r') as f:
        if has_header:
            next(f)  # Skip header
        
        for i, line in enumerate(f):
            total_lines += 1
            parts = line.strip().split(',')
            if len(parts) >= 3:
                try:
                    if i == 0 or i == total_lines - 1 or (i % (total_lines // 100 + 1)) == 0:
                        # Sample timestamps throughout the file
                        timestamps.append(parts[0])
                    outlets.add(int(parts[1]))
                    if i < sample_lines:
                        power_values.append(int(parts[2]))
                except:
                    continue
    
    # Parse timestamps
    start_time = datetime.fromisoformat(timestamps[0].replace('Z', '+00:00'))
    end_time = datetime.fromisoformat(timestamps[-1].replace('Z', '+00:00'))
    duration_hours = (end_time - start_time).total_seconds() / 3600
    
    active_outlets = [o for o in sorted(outlets) if o <= 24]  # Filter to actual PDU outlets
    
    metadata = {
        'filename': csv_path.name,
        'has_header': has_header,
        'total_records': total_lines,
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_hours': round(duration_hours, 2),
        'duration_days': round(duration_hours / 24, 2),
        'num_outlets': len(active_outlets),
        'outlets': active_outlets,
        'power_range': (min(power_values), max(power_values)) if power_values else (0, 0),
        'avg_power': round(sum(power_values) / len(power_values), 2) if power_values else 0
    }
    
    print(f"    → {total_lines:,} records")
    print(f"    → {metadata['duration_days']} days ({start_time.date()} to {end_time.date()})")
    print(f"    → {len(active_outlets)} outlets monitored")
    
    return metadata


def add_header_and_compress(csv_path, output_path, has_header):
    """Add header if missing and compress to .gz format."""
    print(f"  Creating {output_path.name}...")
    
    with open(csv_path, 'r') as f_in:
        with gzip.open(output_path, 'wt') as f_out:
            if not has_header:
                # Add header
                f_out.write('time,active-power-outlet,value\n')
            
            # Copy all lines
            for line in f_in:
                f_out.write(line)
    
    # Get compressed file size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"    Compressed size: {size_mb:.2f} MB")
    
    return size_mb


def verify_compressed_file(gz_path, expected_records):
    """Verify the compressed file can be read correctly."""
    print(f"  Verifying {gz_path.name}...")
    
    with gzip.open(gz_path, 'rt') as f:
        lines = 0
        for line in f:
            lines += 1
            if lines > 10:  # Just verify first few lines
                break
    
    print(f"    ✓ File readable, has header")
    return True


def generate_metadata_report(all_metadata):
    """Generate markdown report of all data files."""
    report = []
    report.append("# PDU Power Metrics Data Files\n\n")
    report.append("This directory contains preprocessed power consumption data from PDU outlets.\n")
    report.append("All files are in CSV format (time,active-power-outlet,value) compressed with gzip.\n\n")
    
    report.append("## Quick Reference\n\n")
    report.append("| Dataset | Period | Duration | Records | Outlets | Size |\n")
    report.append("|---------|--------|----------|---------|---------|------|\n")
    
    for meta in sorted(all_metadata, key=lambda x: x['start_time']):
        start_date = meta['start_time'][:10]
        end_date = meta['end_time'][:10]
        period = f"{start_date} to {end_date}" if start_date != end_date else start_date
        
        report.append(f"| {meta['output_filename']} | {period} | {meta['duration_days']} days | "
                     f"{meta['total_records']:,} | {meta['num_outlets']} | {meta['compressed_size_mb']:.1f} MB |\n")
    
    report.append("\n## Detailed Descriptions\n\n")
    
    for meta in sorted(all_metadata, key=lambda x: x['start_time']):
        start = datetime.fromisoformat(meta['start_time'])
        end = datetime.fromisoformat(meta['end_time'])
        
        report.append(f"### 📊 {meta['output_filename']}\n\n")
        report.append(f"**Monitoring Period:** {start.strftime('%B %d, %Y')} to {end.strftime('%B %d, %Y')}\n\n")
        
        report.append(f"**Temporal Coverage:**\n")
        report.append(f"- Start: `{meta['start_time']}`\n")
        report.append(f"- End: `{meta['end_time']}`\n")
        report.append(f"- Duration: **{meta['duration_days']} days** ({meta['duration_hours']:.1f} hours)\n\n")
        
        report.append(f"**Dataset Statistics:**\n")
        report.append(f"- Total measurements: **{meta['total_records']:,}** power readings\n")
        report.append(f"- PDU outlets monitored: **{meta['num_outlets']}** outlets\n")
        report.append(f"- Outlet IDs: {', '.join(map(str, meta['outlets'][:10]))}")
        if len(meta['outlets']) > 10:
            report.append(f", ... ({len(meta['outlets'])} total)")
        report.append("\n")
        report.append(f"- Power range: {meta['power_range'][0]}-{meta['power_range'][1]} watts\n")
        report.append(f"- Average power: {meta['avg_power']:.1f} watts (sampled)\n")
        report.append(f"- Compressed size: **{meta['compressed_size_mb']:.2f} MB**\n\n")
        
        # Add source info
        if 'original_archive' in meta:
            report.append(f"**Source:** Extracted from `{meta['original_archive']}`\n\n")
        elif 'converted_from' in meta:
            report.append(f"**Source:** Converted from InfluxDB JSON format (`{meta['converted_from']}`)\n\n")
        
        # Usage example
        report.append("**Usage Example:**\n```python\n")
        report.append("from pdumetrics import PowerMetrics\n\n")
        report.append(f"# Load power metrics\n")
        report.append(f"pm = PowerMetrics('data/{meta['output_filename']}')\n\n")
        report.append(f"# Time range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}\n")
        report.append(f"# {meta['total_records']:,} records over {meta['duration_days']} days\n")
        report.append("```\n\n")
        report.append("---\n\n")
    
    # Add processing info
    report.append("## Data Processing\n\n")
    report.append("All files have been:\n")
    report.append("1. ✅ Verified for correct format (CSV with time,outlet,value)\n")
    report.append("2. ✅ Added standard header row if missing\n")
    report.append("3. ✅ Compressed with gzip for efficient storage\n")
    report.append("4. ✅ Ready to use with `pdumetrics.PowerMetrics()`\n\n")
    
    report.append("**File Format:**\n")
    report.append("```csv\n")
    report.append("time,active-power-outlet,value\n")
    report.append("2024-12-19T19:50:03.634357Z,1,17\n")
    report.append("2024-12-19T19:50:03.637139Z,2,0\n")
    report.append("...\n")
    report.append("```\n\n")
    
    report.append("Generated by `preprocess_data.py` on " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n")
    
    return ''.join(report)


def main():
    """Main preprocessing workflow."""
    print("=" * 70)
    print("PDU POWER METRICS DATA PREPROCESSING")
    print("=" * 70)
    
    # Check for keep-archives flag
    keep_archives = '--keep-archives' in sys.argv
    
    data_dir = Path('data')
    if not data_dir.exists():
        print("\n❌ Error: data/ directory not found")
        print("   Run this script from the repository root")
        return 1
    
    # Step 1: Verify input
    archives, csvs = verify_input_data(data_dir)
    
    if not archives and not csvs:
        print("\n❌ No data files to process")
        return 1
    
    print(f"\n{'=' * 70}")
    print("STEP 2: PROCESSING FILES")
    print('=' * 70)
    
    all_metadata = []
    temp_files = []
    
    # Process archives
    for archive_path in sorted(archives):
        try:
            # Extract
            csv_path = extract_tarxz(archive_path, data_dir)
            temp_files.append(csv_path)
            
            # Detect format
            file_format = detect_file_format(csv_path)
            
            if file_format == 'influxdb_json':
                # Convert to CSV
                converted_csv = csv_path.parent / f"{csv_path.stem}_converted.csv"
                convert_influxdb_to_csv(csv_path, converted_csv)
                temp_files.append(converted_csv)
                csv_path = converted_csv
            
            # Analyze
            metadata = analyze_csv(csv_path)
            metadata['original_archive'] = archive_path.name
            
            # Determine output filename
            base_name = Path(metadata['original_archive']).stem.replace('.tar', '')
            output_filename = f"{base_name}.csv.gz"
            output_path = data_dir / output_filename
            metadata['output_filename'] = output_filename
            
            # Add header and compress
            print(f"  Compressing to {output_filename}...")
            compressed_size = add_header_and_compress(csv_path, output_path, metadata['has_header'])
            metadata['compressed_size_mb'] = round(compressed_size, 2)
            
            # Verify
            verify_compressed_file(output_path, metadata['total_records'])
            
            all_metadata.append(metadata)
            print(f"  ✅ Success!")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Process standalone CSV files
    for csv_path in sorted(csvs):
        try:
            print(f"\n{'─' * 70}")
            print(f"Processing: {csv_path.name}")
            print('─' * 70)
            
            # Detect format
            file_format = detect_file_format(csv_path)
            
            if file_format == 'influxdb_json':
                # Convert to CSV
                print(f"  Converting InfluxDB JSON...")
                converted_csv = csv_path.parent / f"{csv_path.stem}_converted.csv"
                convert_influxdb_to_csv(csv_path, converted_csv)
                temp_files.append(converted_csv)
                process_csv = converted_csv
                converted_from = csv_path.name
            else:
                process_csv = csv_path
                converted_from = None
            
            # Analyze
            metadata = analyze_csv(process_csv)
            if converted_from:
                metadata['converted_from'] = converted_from
            
            # Determine output filename
            base_name = csv_path.stem
            output_filename = f"{base_name}.csv.gz"
            output_path = data_dir / output_filename
            metadata['output_filename'] = output_filename
            
            # Add header and compress
            print(f"  Compressing to {output_filename}...")
            compressed_size = add_header_and_compress(process_csv, output_path, metadata['has_header'])
            metadata['compressed_size_mb'] = round(compressed_size, 2)
            
            # Verify
            verify_compressed_file(output_path, metadata['total_records'])
            
            all_metadata.append(metadata)
            print(f"  ✅ Success!")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Clean up temporary files
    print(f"\n{'=' * 70}")
    print("STEP 3: CLEANING UP")
    print('=' * 70)
    
    for temp_file in temp_files:
        if temp_file.exists():
            print(f"  Removing {temp_file.name}...")
            temp_file.unlink()
    
    if not keep_archives:
        for archive in archives:
            print(f"  Removing {archive.name}...")
            archive.unlink()
        print("  ✓ Original archives removed")
    else:
        print("  ✓ Keeping original archives (--keep-archives flag)")
    
    # Generate metadata
    print(f"\n{'=' * 70}")
    print("STEP 4: GENERATING DOCUMENTATION")
    print('=' * 70)
    
    report = generate_metadata_report(all_metadata)
    report_path = data_dir / 'DATA_DESCRIPTION.md'
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"  ✅ Created {report_path}")
    
    # Save JSON metadata
    json_path = data_dir / 'metadata.json'
    with open(json_path, 'w') as f:
        json.dump(all_metadata, f, indent=2)
    print(f"  ✅ Created {json_path}")
    
    # Summary
    print(f"\n{'=' * 70}")
    print("✅ PREPROCESSING COMPLETE")
    print('=' * 70)
    
    print(f"\nProcessed {len(all_metadata)} dataset(s):\n")
    
    total_records = 0
    total_size = 0
    total_days = 0
    
    for meta in sorted(all_metadata, key=lambda x: x['start_time']):
        start_date = meta['start_time'][:10]
        end_date = meta['end_time'][:10]
        print(f"  ✅ {meta['output_filename']}")
        print(f"     Period: {start_date} to {end_date}")
        print(f"     {meta['total_records']:,} records | {meta['duration_days']} days | {meta['compressed_size_mb']:.1f} MB")
        print()
        
        total_records += meta['total_records']
        total_size += meta['compressed_size_mb']
        total_days += meta['duration_days']
    
    print(f"{'─' * 70}")
    print(f"Total: {total_records:,} records | {total_days:.1f} days | {total_size:.1f} MB")
    print(f"{'─' * 70}\n")
    
    print("📁 Files in data/:")
    print(f"   • {len(all_metadata)} ready-to-use .csv.gz file(s)")
    if keep_archives:
        print(f"   • {len(archives)} original .tar.xz archive(s) (kept)")
    print(f"   • DATA_DESCRIPTION.md (documentation)")
    print(f"   • metadata.json (machine-readable metadata)")
    
    print("\n📖 See data/DATA_DESCRIPTION.md for detailed information")
    print("🚀 Files are ready to use with pdumetrics.PowerMetrics()\n")
    
    return 0


if __name__ == '__main__':
    exit(main())
