"""
Apply Safe License Updates
Apply the safe, high-confidence license updates to the integrated dataset
"""

import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def apply_safe_updates():
    print("🛡️  Applying Safe License Updates")
    print("=" * 35)
    
    # Load the integrated dataset
    integrated_df = pd.read_excel('Output/Integrated_BitData_20250702_102059.xlsx')
    logger.info(f"📊 Loaded integrated dataset: {len(integrated_df):,} records")
    
    # Load safe lookup results
    safe_results = pd.read_excel('Output/Safe_License_Lookup_Results_20250702_111951.xlsx')
    logger.info(f"🔍 Loaded safe lookup results: {len(safe_results):,} matches")
    
    missing_before = integrated_df['license_number'].isna().sum()
    logger.info(f"📊 Records missing license before update: {missing_before:,}")
    
    updates_applied = 0
    
    # Apply each safe update
    for _, result in safe_results.iterrows():
        idx = result['original_index']
        license = result['found_license']
        well_name = result['well_name']
        verification = result['verification_details']
        
        if idx < len(integrated_df) and pd.isna(integrated_df.at[idx, 'license_number']):
            integrated_df.at[idx, 'license_number'] = license
            updates_applied += 1
            logger.info(f"✅ Updated index {idx}: {well_name} → License {license} ({verification})")
    
    missing_after = integrated_df['license_number'].isna().sum()
    
    # Save updated dataset
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'Output/Integrated_BitData_SafeUpdated_{timestamp}.xlsx'
    integrated_df.to_excel(output_path, index=False)
    
    print(f"\n📊 SAFE UPDATE SUMMARY:")
    print("=" * 25)
    print(f"  Original missing licenses: {missing_before:,}")
    print(f"  Safe updates applied: {updates_applied}")
    print(f"  Final missing licenses: {missing_after:,}")
    print(f"  Improvement: {missing_before - missing_after} fewer missing licenses")
    print(f"  Success rate: {(updates_applied/missing_before)*100:.1f}%")
    print(f"\n💾 Updated dataset saved to: {output_path}")
    
    return output_path

if __name__ == "__main__":
    apply_safe_updates()
