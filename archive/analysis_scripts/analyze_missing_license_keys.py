"""
Analyze what identifying information is available for wells missing license numbers
to help determine potential lookup keys for finding license numbers.
"""

import pandas as pd
import numpy as np
from pathlib import Path

def analyze_missing_license_keys():
    """Analyze what data is available for records missing license numbers"""
    
    # Find the most recent integrated file
    output_dir = Path("Output")
    integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
    latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
    
    print(f"📊 Analyzing: {latest_file.name}")
    df = pd.read_excel(latest_file)
    
    # Filter to records missing license numbers
    missing_license = df[
        (df['license_number'].isna()) | 
        (df['license_number'] == '') | 
        (df['license_number'].isnull())
    ].copy()
    
    print(f"\n🔍 RECORDS MISSING LICENSE NUMBERS: {len(missing_license):,}")
    print("=" * 60)
    
    # Check data source distribution
    print("📊 BY DATA SOURCE:")
    print(missing_license['data_source'].value_counts())
    
    # Analyze available identifying fields
    identifying_fields = [
        'well_name', 'well_number', 'operator', 'contractor', 'rig_name',
        'field', 'county', 'state_province', 'country',
        'lsd', 'section', 'township', 'range',
        'latitude', 'longitude', 'spud_date', 'td_date'
    ]
    
    print(f"\n🔑 AVAILABLE IDENTIFYING INFORMATION:")
    print("=" * 60)
    
    coverage_stats = {}
    for field in identifying_fields:
        if field in missing_license.columns:
            has_data = missing_license[field].notna() & (missing_license[field] != '')
            count = has_data.sum()
            percent = (count / len(missing_license)) * 100
            coverage_stats[field] = {
                'count': count,
                'percent': percent,
                'sample_values': missing_license[has_data][field].head(5).tolist() if count > 0 else []
            }
            
            print(f"{field:20s}: {count:3d} ({percent:5.1f}%) - {coverage_stats[field]['sample_values'][:3]}")
    
    # Show best combination fields
    print(f"\n🎯 BEST LOOKUP KEY COMBINATIONS:")
    print("=" * 60)
    
    # Well name + operator (most reliable combination)
    has_name_operator = (
        missing_license['well_name'].notna() & (missing_license['well_name'] != '') &
        missing_license['operator'].notna() & (missing_license['operator'] != '')
    )
    print(f"Well Name + Operator: {has_name_operator.sum():3d} ({(has_name_operator.sum()/len(missing_license)*100):5.1f}%)")
    
    # Well name + field + operator
    has_name_field_operator = (
        has_name_operator &
        missing_license['field'].notna() & (missing_license['field'] != '')
    )
    print(f"Well Name + Field + Operator: {has_name_field_operator.sum():3d} ({(has_name_field_operator.sum()/len(missing_license)*100):5.1f}%)")
    
    # Location-based (LSD + Section + Township + Range)
    has_location = (
        missing_license['lsd'].notna() & (missing_license['lsd'] != '') &
        missing_license['section'].notna() & (missing_license['section'] != '') &
        missing_license['township'].notna() & (missing_license['township'] != '') &
        missing_license['range'].notna() & (missing_license['range'] != '')
    )
    print(f"Complete Legal Location (LSD-Sect-TWP-RNG): {has_location.sum():3d} ({(has_location.sum()/len(missing_license)*100):5.1f}%)")
    
    # Coordinates
    has_coords = (
        missing_license['latitude'].notna() & missing_license['longitude'].notna()
    )
    print(f"Latitude + Longitude: {has_coords.sum():3d} ({(has_coords.sum()/len(missing_license)*100):5.1f}%)")
    
    # Show sample records with good identifying info
    print(f"\n📋 SAMPLE RECORDS WITH STRONG IDENTIFIERS:")
    print("=" * 60)
    
    good_records = missing_license[has_name_operator].head(10)
    for i, (idx, row) in enumerate(good_records.iterrows(), 1):
        print(f"\n{i:2d}. Well: {row['well_name']}")
        print(f"    Operator: {row['operator']}")
        print(f"    Field: {row.get('field', 'N/A')}")
        print(f"    Location: {row.get('lsd', 'N/A')}-{row.get('section', 'N/A')}-{row.get('township', 'N/A')}-{row.get('range', 'N/A')}")
        print(f"    Coordinates: {row.get('latitude', 'N/A')}, {row.get('longitude', 'N/A')}")
        print(f"    Spud Date: {row.get('spud_date', 'N/A')}")
    
    # Show unique operators for missing records
    print(f"\n🏢 OPERATORS WITH MISSING LICENSE NUMBERS:")
    print("=" * 60)
    operator_counts = missing_license['operator'].value_counts()
    for operator, count in operator_counts.head(10).items():
        print(f"{operator:30s}: {count:3d} records")
    
    # Show date ranges
    print(f"\n📅 DATE RANGES FOR MISSING RECORDS:")
    print("=" * 60)
    date_fields = ['spud_date', 'td_date', 'run_date']
    for date_field in date_fields:
        if date_field in missing_license.columns:
            valid_dates = missing_license[missing_license[date_field].notna()][date_field]
            if len(valid_dates) > 0:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                print(f"{date_field:12s}: {min_date} to {max_date} ({len(valid_dates)} records)")
    
    # Export sample for manual lookup
    print(f"\n💾 EXPORTING SAMPLE FOR MANUAL LOOKUP:")
    print("=" * 60)
    
    # Create a clean export of the missing records with key fields
    export_fields = [
        'well_name', 'operator', 'field', 'county', 'state_province',
        'lsd', 'section', 'township', 'range', 
        'latitude', 'longitude', 'spud_date', 'td_date'
    ]
    
    export_df = missing_license[export_fields].copy()
    export_file = output_dir / f"Wells_Missing_License_Numbers_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    export_df.to_excel(export_file, index=False)
    
    print(f"✅ Exported {len(export_df)} records to: {export_file.name}")
    print(f"   Fields included: {', '.join(export_fields)}")

if __name__ == "__main__":
    analyze_missing_license_keys()
