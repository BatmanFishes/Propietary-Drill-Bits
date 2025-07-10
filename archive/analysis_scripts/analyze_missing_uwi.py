#!/usr/bin/env python3
"""
Enhanced UWI Lookup using GDC Connection
Fetch missing UWI and UWI formatted fields using the existing GDC Oracle database connection.
"""

import pandas as pd
import os
from pathlib import Path
import sys

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

def analyze_missing_uwi_data():
    """Analyze which records are missing UWI data and need GDC lookup."""
    
    print("🔍 ANALYZING MISSING UWI DATA FOR GDC LOOKUP")
    print("=" * 60)
    
    # Find the latest integrated file
    output_dir = Path("Output")
    files = [f for f in os.listdir(output_dir) if f.startswith("Integrated_BitData_DualUWI_")]
    if not files:
        print("❌ No integrated files found!")
        return None
    
    latest_file = sorted(files)[-1]
    file_path = output_dir / latest_file
    
    print(f"📊 Loading: {latest_file}")
    df = pd.read_excel(file_path)
    
    print(f"📋 Total records: {len(df):,}")
    
    # Analyze UWI coverage by data source
    print(f"\n📊 UWI COVERAGE BY SOURCE:")
    print("-" * 40)
    
    for source in df['data_source'].unique():
        source_data = df[df['data_source'] == source]
        total = len(source_data)
        has_uwi_unformatted = source_data['uwi_number'].notna().sum()
        has_uwi_formatted = source_data['uwi_formatted'].notna().sum()
        has_license = source_data['license_number'].notna().sum()
        
        print(f"  {source}:")
        print(f"    Total records: {total:,}")
        print(f"    Have UWI (unformatted): {has_uwi_unformatted:,} ({has_uwi_unformatted/total*100:.1f}%)")
        print(f"    Have UWI (formatted): {has_uwi_formatted:,} ({has_uwi_formatted/total*100:.1f}%)")
        print(f"    Have license: {has_license:,} ({has_license/total*100:.1f}%)")
    
    # Identify records that need UWI lookup
    missing_uwi = df[
        (df['uwi_number'].isna()) | (df['uwi_formatted'].isna())
    ].copy()
    
    print(f"\n🎯 RECORDS NEEDING UWI LOOKUP:")
    print("-" * 40)
    print(f"  Missing UWI data: {len(missing_uwi):,}")
    
    # Break down by what's available for matching
    has_license_only = missing_uwi[
        (missing_uwi['license_number'].notna()) &
        (missing_uwi['well_name'].notna())
    ]
    
    has_coordinates = missing_uwi[
        (missing_uwi['latitude'].notna()) &
        (missing_uwi['longitude'].notna())
    ]
    
    has_well_name = missing_uwi[missing_uwi['well_name'].notna()]
    
    print(f"  With license number: {len(has_license_only):,}")
    print(f"  With coordinates: {len(has_coordinates):,}")
    print(f"  With well name: {len(has_well_name):,}")
    
    # Show sample records that need UWI lookup
    print(f"\n📋 SAMPLE RECORDS NEEDING UWI LOOKUP:")
    print("-" * 50)
    
    sample_missing = missing_uwi[[
        'well_name', 'license_number', 'operator', 'field', 
        'latitude', 'longitude', 'data_source'
    ]].head(10)
    
    for idx, row in sample_missing.iterrows():
        print(f"  Well: {row['well_name']}")
        print(f"    License: {row['license_number']}")
        print(f"    Operator: {row['operator']}")
        print(f"    Source: {row['data_source']}")
        print(f"    Has Coords: {pd.notna(row['latitude']) and pd.notna(row['longitude'])}")
        print()
    
    # Prepare data for GDC lookup
    gdc_lookup_data = missing_uwi[
        missing_uwi['well_name'].notna()
    ].copy()
    
    print(f"🎯 READY FOR GDC LOOKUP:")
    print("-" * 30)
    print(f"  Records with well names: {len(gdc_lookup_data):,}")
    print(f"  Unique wells: {gdc_lookup_data['well_name'].nunique():,}")
    print(f"  Unique operators: {gdc_lookup_data['operator'].nunique():,}")
    
    # Save subset for GDC processing
    gdc_file = output_dir / "Missing_UWI_for_GDC_Lookup.xlsx"
    gdc_lookup_data.to_excel(gdc_file, index=False)
    print(f"💾 Saved GDC lookup file: {gdc_file.name}")
    
    return gdc_lookup_data

def main():
    """Main function"""
    try:
        missing_data = analyze_missing_uwi_data()
        
        if missing_data is not None:
            print(f"\n✅ Analysis complete!")
            print(f"   Records ready for GDC UWI lookup: {len(missing_data):,}")
            print(f"   Next step: Run GDC lookup to fetch UWI data")
            
    except Exception as e:
        print(f"❌ Error in analysis: {e}")

if __name__ == "__main__":
    main()
