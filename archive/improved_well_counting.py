"""
Improved Well Counting System
Uses the most reliable well identifiers (UWI/API first, then well name as fallback)
"""

import pandas as pd
import numpy as np
from universal_data_integration import DataIntegrationEngine

def create_unified_well_identifier(df):
    """Create a unified well identifier using the best available method"""
    
    # Create a copy to work with
    df = df.copy()
    
    # Clean and standardize UWI/API numbers
    df['clean_uwi'] = df['api_number'].astype(str).str.strip().str.upper()
    df['clean_uwi'] = df['clean_uwi'].replace(['NAN', 'NONE', ''], np.nan)
    
    # Clean well names
    df['clean_well_name'] = df['well_name'].astype(str).str.strip().str.upper()
    df['clean_well_name'] = df['clean_well_name'].replace(['NAN', 'NONE', ''], np.nan)
    
    # Create unified identifier priority:
    # 1. Use UWI/API if available
    # 2. Fall back to standardized well name
    # 3. Use license number as last resort (for Reed data)
    
    df['unified_well_id'] = df['clean_uwi']
    
    # For records without UWI, use well name
    mask_no_uwi = df['unified_well_id'].isna()
    df.loc[mask_no_uwi, 'unified_well_id'] = df.loc[mask_no_uwi, 'clean_well_name']
    
    # For Reed data without UWI or well name, use license
    mask_no_id = df['unified_well_id'].isna()
    reed_mask = df['data_source'] == 'reed'
    use_license = mask_no_id & reed_mask
    if use_license.any():
        df.loc[use_license, 'unified_well_id'] = 'LIC_' + df.loc[use_license, 'license_number'].astype(str)
    
    return df

def analyze_improved_well_counting():
    """Analyze well counting with improved methodology"""
    
    print('=== IMPROVED WELL COUNTING ANALYSIS ===')
    print()

    # Load the integrated data
    engine = DataIntegrationEngine()
    integrated_df = engine.integrate_all_sources()

    # Apply improved well identification
    df_with_unified_id = create_unified_well_identifier(integrated_df)
    
    # Original vs improved counts
    original_count = integrated_df['well_name'].nunique()
    improved_count = df_with_unified_id['unified_well_id'].nunique()
    
    print(f'Original well count (well_name): {original_count:,}')
    print(f'Improved well count (unified_id): {improved_count:,}')
    print(f'Difference: {original_count - improved_count:,} wells')
    print(f'Reduction: {(original_count - improved_count) / original_count * 100:.1f}%')
    
    print('\n=== IDENTIFIER USAGE BREAKDOWN ===')
    
    # Count how many records use each identifier type
    uwi_used = df_with_unified_id['clean_uwi'].notna().sum()
    wellname_used = (df_with_unified_id['clean_uwi'].isna() & 
                     df_with_unified_id['clean_well_name'].notna()).sum()
    license_used = (df_with_unified_id['clean_uwi'].isna() & 
                    df_with_unified_id['clean_well_name'].isna() & 
                    df_with_unified_id['license_number'].notna()).sum()
    
    print(f'Records using UWI/API: {uwi_used:,} ({uwi_used/len(df_with_unified_id)*100:.1f}%)')
    print(f'Records using Well Name: {wellname_used:,} ({wellname_used/len(df_with_unified_id)*100:.1f}%)')
    print(f'Records using License: {license_used:,} ({license_used/len(df_with_unified_id)*100:.1f}%)')
    
    print('\n=== BY DATA SOURCE ===')
    for source in df_with_unified_id['data_source'].unique():
        source_data = df_with_unified_id[df_with_unified_id['data_source'] == source]
        
        original_wells = integrated_df[integrated_df['data_source'] == source]['well_name'].nunique()
        improved_wells = source_data['unified_well_id'].nunique()
        
        print(f'\n{source}:')
        print(f'  Original well count: {original_wells:,}')
        print(f'  Improved well count: {improved_wells:,}')
        print(f'  Difference: {original_wells - improved_wells:,}')
        
        # Show identifier usage for this source
        source_uwi = source_data['clean_uwi'].notna().sum()
        source_wellname = (source_data['clean_uwi'].isna() & 
                          source_data['clean_well_name'].notna()).sum()
        source_license = (source_data['clean_uwi'].isna() & 
                         source_data['clean_well_name'].isna() & 
                         source_data['license_number'].notna()).sum()
        
        print(f'  Using UWI: {source_uwi:,}')
        print(f'  Using Well Name: {source_wellname:,}')
        print(f'  Using License: {source_license:,}')

    # Analysis of overlapping wells
    print('\n=== WELL OVERLAP ANALYSIS ===')
    
    ulterra_wells = set(df_with_unified_id[df_with_unified_id['data_source'] == 'ulterra']['unified_well_id'])
    reed_wells = set(df_with_unified_id[df_with_unified_id['data_source'] == 'reed']['unified_well_id'])
    
    overlap = ulterra_wells.intersection(reed_wells)
    ulterra_only = ulterra_wells - reed_wells
    reed_only = reed_wells - ulterra_wells
    
    print(f'Wells in both sources: {len(overlap):,}')
    print(f'Ulterra only: {len(ulterra_only):,}')
    print(f'Reed only: {len(reed_only):,}')
    print(f'Total unique wells: {len(ulterra_wells.union(reed_wells)):,}')
    
    if len(overlap) > 0:
        print(f'\nSample overlapping wells:')
        for well in list(overlap)[:5]:
            print(f'  {well}')
    
    # Show samples of potential duplicates that were merged
    print('\n=== SAMPLE DUPLICATE DETECTION ===')
    
    # Find cases where multiple well names map to same UWI
    uwi_to_names = df_with_unified_id[df_with_unified_id['clean_uwi'].notna()].groupby('clean_uwi')['clean_well_name'].nunique()
    multiple_names = uwi_to_names[uwi_to_names > 1]
    
    if len(multiple_names) > 0:
        print(f'UWIs with multiple well names: {len(multiple_names)}')
        print('Sample cases:')
        for uwi in multiple_names.head(3).index:
            names = df_with_unified_id[df_with_unified_id['clean_uwi'] == uwi]['clean_well_name'].unique()
            print(f'  UWI {uwi}:')
            for name in names[:3]:
                if pd.notna(name):
                    print(f'    - {name}')

    return df_with_unified_id

def update_quality_report_with_improved_counting():
    """Update the quality reporting to use improved well counting"""
    
    # This function shows how to modify the integration engine
    print('\n=== RECOMMENDATION FOR SYSTEM UPDATE ===')
    print()
    print('To improve well counting accuracy, update the DataIntegrationEngine:')
    print()
    print('1. Add unified_well_id creation in _add_derived_fields()')
    print('2. Update get_data_quality_report() to use unified_well_id')
    print('3. Update _create_source_summary() to use unified_well_id')
    print()
    print('Benefits:')
    print('- More accurate well counts')
    print('- Better duplicate detection')
    print('- Improved cross-source analysis')
    print('- Standardized well identification')

if __name__ == "__main__":
    df_with_improved_ids = analyze_improved_well_counting()
    update_quality_report_with_improved_counting()
