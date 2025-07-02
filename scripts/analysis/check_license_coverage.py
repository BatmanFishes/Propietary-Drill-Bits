"""
Quick analysis of license number coverage in the integrated dataset
"""

import pandas as pd
import numpy as np
from pathlib import Path

def analyze_license_coverage():
    """Analyze license number coverage by source"""
    
    # Find the most recent integrated file
    output_dir = Path("Output")
    if not output_dir.exists():
        print("❌ Output directory not found")
        return
    
    # Get the most recent integrated file
    integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
    if not integrated_files:
        print("❌ No integrated data files found")
        return
    
    latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
    print(f"📊 Analyzing: {latest_file.name}")
    
    # Load the data
    df = pd.read_excel(latest_file)
    
    print(f"\n📈 Total Records: {len(df):,}")
    print(f"📈 Total Sources: {df['data_source'].nunique()}")
    
    # Analyze license number coverage
    print("\n🏷️  LICENSE NUMBER COVERAGE:")
    print("=" * 50)
    
    # Overall coverage
    has_license = df['license_number'].notna() & (df['license_number'] != '')
    license_count = has_license.sum()
    license_percent = (license_count / len(df)) * 100
    
    print(f"Records with license numbers: {license_count:,} ({license_percent:.1f}%)")
    print(f"Records without license numbers: {(len(df) - license_count):,} ({(100 - license_percent):.1f}%)")
    
    # By source
    print("\n📊 BY DATA SOURCE:")
    print("-" * 30)
    for source in df['data_source'].unique():
        source_df = df[df['data_source'] == source]
        source_has_license = source_df['license_number'].notna() & (source_df['license_number'] != '')
        source_license_count = source_has_license.sum()
        source_license_percent = (source_license_count / len(source_df)) * 100
        
        print(f"{source.upper()}:")
        print(f"  Total records: {len(source_df):,}")
        print(f"  With license: {source_license_count:,} ({source_license_percent:.1f}%)")
        print(f"  Without license: {(len(source_df) - source_license_count):,} ({(100 - source_license_percent):.1f}%)")
        print()
    
    # UWI coverage (for comparison)
    print("🏷️  UWI NUMBER COVERAGE (for comparison):")
    print("=" * 50)
    
    has_uwi = df['uwi_number'].notna() & (df['uwi_number'] != '')
    uwi_count = has_uwi.sum()
    uwi_percent = (uwi_count / len(df)) * 100
    
    print(f"Records with UWI numbers: {uwi_count:,} ({uwi_percent:.1f}%)")
    print(f"Records without UWI numbers: {(len(df) - uwi_count):,} ({(100 - uwi_percent):.1f}%)")
    
    # By source
    print("\n📊 BY DATA SOURCE:")
    print("-" * 30)
    for source in df['data_source'].unique():
        source_df = df[df['data_source'] == source]
        source_has_uwi = source_df['uwi_number'].notna() & (source_df['uwi_number'] != '')
        source_uwi_count = source_has_uwi.sum()
        source_uwi_percent = (source_uwi_count / len(source_df)) * 100
        
        print(f"{source.upper()}:")
        print(f"  Total records: {len(source_df):,}")
        print(f"  With UWI: {source_uwi_count:,} ({source_uwi_percent:.1f}%)")
        print(f"  Without UWI: {(len(source_df) - source_uwi_count):,} ({(100 - source_uwi_percent):.1f}%)")
        print()
    
    # Combined identifier coverage
    print("🏷️  COMBINED IDENTIFIER COVERAGE:")
    print("=" * 50)
    
    has_any_id = has_uwi | has_license | (df['well_name'].notna() & (df['well_name'] != ''))
    any_id_count = has_any_id.sum()
    any_id_percent = (any_id_count / len(df)) * 100
    
    print(f"Records with any identifier (UWI, License, or Name): {any_id_count:,} ({any_id_percent:.1f}%)")
    
    # Show some sample license numbers
    if license_count > 0:
        print("\n📋 SAMPLE LICENSE NUMBERS:")
        print("-" * 30)
        sample_licenses = df[has_license]['license_number'].head(10).tolist()
        for i, lic in enumerate(sample_licenses, 1):
            print(f"{i:2d}. {lic}")

if __name__ == "__main__":
    analyze_license_coverage()
