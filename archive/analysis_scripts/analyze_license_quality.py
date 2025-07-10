#!/usr/bin/env python3
"""
License Number Quality Analysis and Standardization
Analyze license number formats and standardize them with proper leading zeros
"""

import pandas as pd
import numpy as np
from datetime import datetime
import re

def analyze_license_quality():
    """Analyze license number quality and patterns"""
    
    # Load the integrated data
    df = pd.read_excel('Output/test_gdc_uwi_lookup.xlsx')
    print(f"Total records: {len(df)}")
    
    # Focus on records with license numbers
    licensed_records = df[df['license_number'].notna()].copy()
    print(f"Records with license numbers: {len(licensed_records)}")
    
    # Analyze license number patterns
    print("\n=== License Number Analysis ===")
    
    # Convert to string and clean
    licensed_records['license_clean'] = licensed_records['license_number'].astype(str).str.strip()
    
    # Analyze lengths
    licensed_records['license_length'] = licensed_records['license_clean'].str.len()
    length_distribution = licensed_records['license_length'].value_counts().sort_index()
    
    print("License number length distribution:")
    for length, count in length_distribution.items():
        pct = count / len(licensed_records) * 100
        print(f"  {length} chars: {count:,} records ({pct:.1f}%)")
    
    # Analyze by data source
    print("\nLicense length by data source:")
    for source in licensed_records['data_source'].unique():
        source_data = licensed_records[licensed_records['data_source'] == source]
        avg_length = source_data['license_length'].mean()
        print(f"  {source}: avg length = {avg_length:.1f}")
        
        # Show distribution for this source
        source_lengths = source_data['license_length'].value_counts().sort_index()
        for length, count in source_lengths.items():
            pct = count / len(source_data) * 100
            print(f"    {length} chars: {count:,} ({pct:.1f}%)")
    
    # Check for numeric vs non-numeric
    licensed_records['is_numeric'] = licensed_records['license_clean'].str.isnumeric()
    numeric_count = licensed_records['is_numeric'].sum()
    print(f"\nNumeric licenses: {numeric_count}/{len(licensed_records)} ({numeric_count/len(licensed_records)*100:.1f}%)")
    
    # Show samples by length
    print("\n=== Sample License Numbers by Length ===")
    for length in sorted(licensed_records['license_length'].unique()):
        samples = licensed_records[licensed_records['license_length'] == length]['license_clean'].head(10).tolist()
        print(f"\nLength {length} samples:")
        for i, sample in enumerate(samples, 1):
            print(f"  {i}: {sample}")
    
    # Check records missing UWI
    missing_uwi = licensed_records[licensed_records['uwi_number'].isna()]
    print(f"\n=== Records with License but Missing UWI ===")
    print(f"Count: {len(missing_uwi)}")
    
    if len(missing_uwi) > 0:
        print("License length distribution for missing UWI records:")
        missing_lengths = missing_uwi['license_length'].value_counts().sort_index()
        for length, count in missing_lengths.items():
            pct = count / len(missing_uwi) * 100
            print(f"  {length} chars: {count} records ({pct:.1f}%)")
        
        print("\nSample licenses missing UWI:")
        sample_missing = missing_uwi[['license_clean', 'license_length', 'data_source', 'well_name']].head(20)
        print(sample_missing.to_string(index=False))
    
    return licensed_records

def standardize_license_numbers():
    """Standardize license numbers with proper leading zeros"""
    
    # Load the integrated data
    df = pd.read_excel('Output/test_gdc_uwi_lookup.xlsx')
    print(f"\n=== License Number Standardization ===")
    print(f"Processing {len(df)} records...")
    
    def standardize_license(license_val):
        """Standardize a single license number"""
        if pd.isna(license_val):
            return None
        
        # Convert to string and clean
        license_str = str(license_val).strip()
        
        # If it's empty or invalid, return None
        if not license_str or license_str.lower() in ['nan', 'none', '']:
            return None
        
        # Check if it's numeric
        if license_str.isnumeric():
            # Convert to integer to remove any decimal places
            license_int = int(license_str)
            
            # Canadian drilling licenses are typically 5-6 digits
            # Pad to 6 digits with leading zeros
            standardized = f"{license_int:06d}"
            return standardized
        else:
            # Non-numeric license - return as is but cleaned
            return license_str
    
    # Apply standardization
    original_licenses = df['license_number'].copy()
    df['license_number_standardized'] = df['license_number'].apply(standardize_license)
    
    # Compare before and after
    changed_mask = (original_licenses.astype(str) != df['license_number_standardized'].astype(str))
    changed_count = changed_mask.sum()
    
    print(f"License numbers changed: {changed_count}")
    
    if changed_count > 0:
        print("\nSample changes:")
        changed_records = df[changed_mask][['license_number', 'license_number_standardized', 'data_source', 'well_name']].head(20)
        print(changed_records.to_string(index=False))
    
    # Update the original column
    df['license_number'] = df['license_number_standardized']
    df.drop('license_number_standardized', axis=1, inplace=True)
    
    # Show new distribution
    licensed_records = df[df['license_number'].notna()].copy()
    licensed_records['license_length'] = licensed_records['license_number'].astype(str).str.len()
    new_length_distribution = licensed_records['license_length'].value_counts().sort_index()
    
    print(f"\nNew license length distribution:")
    for length, count in new_length_distribution.items():
        pct = count / len(licensed_records) * 100
        print(f"  {length} chars: {count:,} records ({pct:.1f}%)")
    
    # Save the corrected data
    output_file = f'Output/integrated_data_corrected_licenses_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"\nCorrected data saved to: {output_file}")
    
    return df, output_file

def export_uwi_failures():
    """Export the 402 UWI failures for analysis"""
    
    # Load the data
    df = pd.read_excel('Output/test_gdc_uwi_lookup.xlsx')
    
    # Find records with missing UWI
    missing_uwi = df[df['uwi_number'].isna()].copy()
    print(f"\nExporting {len(missing_uwi)} UWI failure records...")
    
    # Select relevant columns for analysis
    export_columns = [
        'data_source', 'license_number', 'uwi_number', 'uwi_formatted', 
        'well_name', 'operator', 'field', 'spud_date', 'source_file'
    ]
    
    # Ensure columns exist
    available_columns = [col for col in export_columns if col in missing_uwi.columns]
    
    export_data = missing_uwi[available_columns].copy()
    
    # Add analysis columns
    export_data['license_length'] = export_data['license_number'].astype(str).str.len()
    export_data['license_is_numeric'] = export_data['license_number'].astype(str).str.isnumeric()
    
    # Sort by data source and license number
    export_data = export_data.sort_values(['data_source', 'license_number'])
    
    # Export to Excel
    failure_file = f'Output/uwi_failures_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    export_data.to_excel(failure_file, index=False, engine='openpyxl')
    
    print(f"UWI failures exported to: {failure_file}")
    
    # Show summary
    print(f"\nUWI Failure Summary:")
    print(f"Total failures: {len(export_data)}")
    
    by_source = export_data.groupby('data_source').size()
    print("\nBy data source:")
    for source, count in by_source.items():
        print(f"  {source}: {count}")
    
    by_length = export_data.groupby('license_length').size()
    print("\nBy license length:")
    for length, count in by_length.items():
        print(f"  {length} chars: {count}")
    
    return export_data, failure_file

if __name__ == "__main__":
    # Analyze current license quality
    licensed_records = analyze_license_quality()
    
    # Export UWI failures for analysis
    failures, failure_file = export_uwi_failures()
    
    # Standardize license numbers
    corrected_df, corrected_file = standardize_license_numbers()
    
    print(f"\n=== Summary ===")
    print(f"UWI failures exported to: {failure_file}")
    print(f"Corrected data saved to: {corrected_file}")
    print(f"\nNext steps:")
    print(f"1. Review the UWI failures file to understand patterns")
    print(f"2. Re-run integration with corrected license numbers")
    print(f"3. Test GDC lookup with standardized licenses")
