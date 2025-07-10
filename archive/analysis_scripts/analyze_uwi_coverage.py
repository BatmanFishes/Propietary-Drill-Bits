#!/usr/bin/env python3
"""
Analyze UWI coverage in the final integrated dataset and identify records 
that need UWI data fetched.
"""

import pandas as pd
import os
from pathlib import Path

def analyze_uwi_coverage():
    """Analyze UWI coverage and identify records needing UWI lookup."""
    
    # Find the latest integrated file
    output_dir = "Output"
    files = [f for f in os.listdir(output_dir) if f.startswith("Integrated_BitData_DualUWI_")]
    if not files:
        print("❌ No integrated files found!")
        return
    
    latest_file = sorted(files)[-1]
    file_path = os.path.join(output_dir, latest_file)
    
    print(f"📊 Analyzing UWI coverage in: {latest_file}")
    print("=" * 70)
    
    # Load the data
    df = pd.read_excel(file_path)
    
    print(f"📋 Total records: {len(df):,}")
    
    # Analyze UWI coverage
    has_uwi_unformatted = df['uwi_number'].notna()
    has_uwi_formatted = df['uwi_formatted'].notna()
    has_license = df['license_number'].notna()
    
    print(f"🔢 Records with uwi_number: {has_uwi_unformatted.sum():,} ({has_uwi_unformatted.sum()/len(df)*100:.1f}%)")
    print(f"📝 Records with uwi_formatted: {has_uwi_formatted.sum():,} ({has_uwi_formatted.sum()/len(df)*100:.1f}%)")
    print(f"📋 Records with license_number: {has_license.sum():,} ({has_license.sum()/len(df)*100:.1f}%)")
    
    # Find records missing UWI but having license numbers
    missing_uwi_with_license = df[has_license & (~has_uwi_unformatted | ~has_uwi_formatted)]
    
    print(f"\n🎯 Records with license but missing UWI data: {len(missing_uwi_with_license):,}")
    
    if len(missing_uwi_with_license) > 0:
        print("\n📊 Breakdown by data source:")
        source_breakdown = missing_uwi_with_license['data_source'].value_counts()
        for source, count in source_breakdown.items():
            print(f"   {source}: {count:,} records")
        
        print("\n📋 Sample records missing UWI data:")
        sample_cols = ['well_name', 'license_number', 'uwi_number', 'uwi_formatted', 'operator', 'data_source']
        sample = missing_uwi_with_license[sample_cols].head(10)
        for idx, row in sample.iterrows():
            print(f"   Well: {row['well_name']}")
            print(f"   License: {row['license_number']}")
            print(f"   UWI (unformatted): {row['uwi_number']}")
            print(f"   UWI (formatted): {row['uwi_formatted']}")
            print(f"   Operator: {row['operator']}")
            print(f"   Source: {row['data_source']}")
            print("   ---")
        
        # Save records needing UWI lookup
        output_file = os.path.join(output_dir, "Records_Needing_UWI_Lookup.xlsx")
        missing_uwi_with_license.to_excel(output_file, index=False)
        print(f"\n💾 Records needing UWI lookup saved to: {output_file}")
        
        # Get unique license numbers for lookup
        unique_licenses = missing_uwi_with_license['license_number'].dropna().unique()
        print(f"\n🔍 Unique license numbers needing UWI lookup: {len(unique_licenses):,}")
        
        # Save unique license numbers for lookup
        license_file = os.path.join(output_dir, "License_Numbers_For_UWI_Lookup.xlsx")
        pd.DataFrame({'license_number': unique_licenses}).to_excel(license_file, index=False)
        print(f"💾 License numbers for lookup saved to: {license_file}")
    
    else:
        print("\n✅ All records with license numbers already have UWI data!")
    
    print("=" * 70)
    print("🎉 UWI coverage analysis complete!")

if __name__ == "__main__":
    analyze_uwi_coverage()
