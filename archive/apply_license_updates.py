"""
Apply License Updates to Integrated Dataset
Apply the license recommendations from GDC lookup to the integrated bit data
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def apply_license_updates():
    """Apply license recommendations to the integrated dataset"""
    
    print("🔄 Applying License Updates to Integrated Dataset")
    print("=" * 55)
    
    # Find the latest files
    output_dir = Path("Output")
    
    # Find latest integrated dataset
    integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
    if not integrated_files:
        logger.error("❌ No integrated dataset found!")
        return
    
    latest_integrated = max(integrated_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"📊 Loading integrated dataset: {latest_integrated.name}")
    
    # Find latest license lookup results
    lookup_files = list(output_dir.glob("License_Lookup_Results_*.xlsx"))
    if not lookup_files:
        logger.error("❌ No license lookup results found!")
        return
    
    latest_lookup = max(lookup_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"🔍 Loading license lookup results: {latest_lookup.name}")
    
    # Load the datasets
    try:
        integrated_df = pd.read_excel(latest_integrated)
        lookup_df = pd.read_excel(latest_lookup)
        
        logger.info(f"📊 Integrated dataset: {len(integrated_df):,} records")
        logger.info(f"🔍 License recommendations: {len(lookup_df):,} records")
        
    except Exception as e:
        logger.error(f"❌ Error loading files: {e}")
        return
    
    # Check current license status
    missing_before = integrated_df['license_number'].isna().sum()
    logger.info(f"📊 Records missing license before update: {missing_before:,}")
    
    # Apply updates
    updates_applied = 0
    
    for _, lookup_row in lookup_df.iterrows():
        original_index = lookup_row['original_index']
        recommended_license = lookup_row['found_license']
        match_method = lookup_row['match_method']
        confidence = lookup_row['confidence']
        similarity = lookup_row['similarity_score']
        
        # Check if this record exists and needs updating
        if original_index < len(integrated_df):
            current_license = integrated_df.at[original_index, 'license_number']
            
            if pd.isna(current_license):
                # Apply the update
                integrated_df.at[original_index, 'license_number'] = recommended_license
                
                # Log the update
                well_name = integrated_df.at[original_index, 'well_name']
                logger.info(f"✅ Updated index {original_index}: {well_name} → License {recommended_license} ({match_method}, {confidence}, {similarity:.3f})")
                updates_applied += 1
            else:
                logger.warning(f"⚠️  Index {original_index} already has license {current_license}, skipping")
        else:
            logger.warning(f"⚠️  Index {original_index} not found in integrated dataset")
    
    # Check final status
    missing_after = integrated_df['license_number'].isna().sum()
    logger.info(f"📊 Records missing license after update: {missing_after:,}")
    logger.info(f"✅ Total updates applied: {updates_applied}")
    logger.info(f"📈 Improvement: {missing_before - missing_after} fewer missing licenses")
    
    # Save updated dataset
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"Integrated_BitData_Updated_{timestamp}.xlsx"
    output_path = output_dir / output_filename
    
    try:
        integrated_df.to_excel(output_path, index=False)
        logger.info(f"💾 Updated dataset saved to: {output_filename}")
        
        # Summary statistics
        print(f"\n📊 UPDATE SUMMARY:")
        print("=" * 30)
        print(f"  Original missing licenses: {missing_before:,}")
        print(f"  Updates applied: {updates_applied}")
        print(f"  Final missing licenses: {missing_after:,}")
        print(f"  Improvement: {missing_before - missing_after} fewer missing licenses")
        print(f"  Success rate: {(updates_applied/missing_before)*100:.1f}%")
        
        return output_path
        
    except Exception as e:
        logger.error(f"❌ Error saving updated dataset: {e}")
        return None

if __name__ == "__main__":
    result = apply_license_updates()
    if result:
        print(f"\n✅ License updates successfully applied!")
        print(f"📁 Updated file: {result}")
    else:
        print(f"\n❌ Failed to apply license updates")
