#!/usr/bin/env python3
"""
Enhanced Bit Manufacturer Name Cleanup Script
Maps actual manufacturer names found in the dataset according to business rules
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedManufacturerCleanup:
    def __init__(self):
        self.output_dir = Path("Output")
        self.df = None
        
    def load_latest_dataset(self):
        """Load the most recent cleaned dataset"""
        # Look for the most recent cleaned manufacturers file first
        cleaned_files = list(self.output_dir.glob("Integrated_BitData_CleanedManufacturers_*.xlsx"))
        safe_files = list(self.output_dir.glob("Integrated_BitData_SafeUpdated_*.xlsx"))
        regular_files = list(self.output_dir.glob("Integrated_BitData_*.xlsx"))
        
        # Filter out SafeUpdated and CleanedManufacturers from regular files
        regular_files = [f for f in regular_files if "SafeUpdated" not in f.name and "CleanedManufacturers" not in f.name]
        
        if cleaned_files:
            latest_file = max(cleaned_files, key=lambda x: x.stat().st_mtime)
        elif safe_files:
            latest_file = max(safe_files, key=lambda x: x.stat().st_mtime)
        elif regular_files:
            latest_file = max(regular_files, key=lambda x: x.stat().st_mtime)
        else:
            raise FileNotFoundError("No integrated data files found")
        
        logger.info(f"📊 Loading dataset: {latest_file.name}")
        self.df = pd.read_excel(latest_file)
        logger.info(f"📈 Total records loaded: {len(self.df):,}")
        
        return latest_file.name
    
    def create_enhanced_mapping(self):
        """Create enhanced manufacturer mapping based on actual data and business rules"""
        
        # Based on the actual manufacturer names in the dataset and business consolidation rules
        mapping = {
            # Reed consolidation (NOV and Reed Hycalog are the same company)
            'REEDHYCALOG': 'Reed',
            'Reed': 'Reed',
            'NOV': 'Reed',
            
            # Smith consolidation (Schlumberger and Smith are the same source)
            'SMITH BITS': 'Smith',
            'SLB': 'Smith',  # Schlumberger company code
            
            # Security consolidation (Halliburton and Security are the same)
            'SECURITY DIAMANT BOART STRATABIT': 'Security',
            'HAL': 'Security',  # Halliburton company code
            
            # Baker Hughes consolidation
            'HUGHES CHRISTENSEN': 'Baker Hughes',
            'BH': 'Baker Hughes',  # Baker Hughes company code
            
            # Ulterra standardization
            'Ulterra': 'Ulterra',
            'ULT': 'Ulterra',  # Ulterra company code
            
            # Varel standardization
            'VAREL INTERNATIONAL': 'Varel',
            'VAR': 'Varel',  # Varel company code
            
            # Kingdream standardization
            'KING DREAM': 'Kingdream',
            'KD': 'Kingdream',  # Kingdream company code
            
            # Other manufacturers - keep as is but clean up
            'DRILFORMANCE': 'Drilformance',
            'DF': 'Drilformance',  # Likely Drilformance code
            
            'SHEAR BITS': 'Shear Bits',
            'SHR': 'Shear Bits',  # Likely Shear Bits code
            
            'J AND L SUPPLY CO. LTD.': 'J&L Supply',
            'TAUREX': 'Taurex',
            'TRX': 'Taurex',  # Likely Taurex code
            
            'TRENDON': 'Trendon',
            'SANDVIK': 'Sandvik',
            'KITTERS BIT SUPPLY': 'Kitters Bit Supply',
            'HAC': 'HAC',
            'DRM': 'DRM',
            
            # Handle generic/unknown
            'OTHER': 'Other',
            'OTH': 'Other',
            'Unknown': 'Unknown',
            'UNKNOWN': 'Unknown'
        }
        
        print(f"\n🔄 ENHANCED MANUFACTURER CONSOLIDATION PLAN:")
        print("=" * 60)
        
        # Group by target company
        consolidations = {}
        for original, target in mapping.items():
            if target not in consolidations:
                consolidations[target] = []
            consolidations[target].append(original)
        
        for target, sources in consolidations.items():
            if len(sources) > 1:
                print(f"  {target:<15}: {', '.join(sources)}")
            else:
                print(f"  {target:<15}: {sources[0]} (no change)")
        
        return mapping
    
    def apply_enhanced_cleanup(self):
        """Apply the enhanced manufacturer consolidation"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return 0
        
        mapping = self.create_enhanced_mapping()
        
        if 'bit_manufacturer' not in self.df.columns:
            logger.error("❌ bit_manufacturer column not found")
            return 0
        
        # Create backup if not already exists
        if 'bit_manufacturer_original' not in self.df.columns:
            self.df['bit_manufacturer_original'] = self.df['bit_manufacturer'].copy()
        
        # Apply mapping
        changes_made = 0
        for idx, current_value in enumerate(self.df['bit_manufacturer']):
            if current_value in mapping:
                new_value = mapping[current_value]
                if new_value != current_value:
                    self.df.at[idx, 'bit_manufacturer'] = new_value
                    changes_made += 1
        
        logger.info(f"✅ Enhanced cleanup applied: {changes_made:,} records changed")
        return changes_made
    
    def analyze_final_results(self):
        """Analyze the final consolidation results"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        print(f"\n📊 FINAL MANUFACTURER CONSOLIDATION RESULTS:")
        print("=" * 60)
        
        final_counts = self.df['bit_manufacturer'].value_counts()
        
        for i, (manufacturer, count) in enumerate(final_counts.items(), 1):
            percentage = (count / len(self.df)) * 100
            print(f"  {i:2d}. {manufacturer:<20} - {count:,} records ({percentage:.1f}%)")
        
        print(f"\n📈 CONSOLIDATION SUMMARY:")
        print(f"  Final unique manufacturers: {len(final_counts)}")
        print(f"  Total records: {len(self.df):,}")
        
        # Show major consolidations
        major_companies = ['Reed', 'Smith', 'Security', 'Baker Hughes', 'Ulterra']
        total_major = 0
        
        print(f"\n🏢 MAJOR COMPANY CONSOLIDATIONS:")
        for company in major_companies:
            count = final_counts.get(company, 0)
            if count > 0:
                percentage = (count / len(self.df)) * 100
                print(f"  {company:<15}: {count:,} records ({percentage:.1f}%)")
                total_major += count
        
        other_percentage = ((len(self.df) - total_major) / len(self.df)) * 100
        print(f"  {'Other companies':<15}: {len(self.df) - total_major:,} records ({other_percentage:.1f}%)")
    
    def save_final_dataset(self):
        """Save the final consolidated dataset"""
        if self.df is None:
            logger.error("❌ No dataset to save")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"Integrated_BitData_ConsolidatedManufacturers_{timestamp}.xlsx"
        output_path = self.output_dir / output_filename
        
        logger.info(f"💾 Saving final consolidated dataset: {output_filename}")
        self.df.to_excel(output_path, index=False)
        
        print(f"\n💾 FINAL CONSOLIDATED DATASET SAVED:")
        print(f"   File: {output_filename}")
        print(f"   Records: {len(self.df):,}")
        print(f"   Unique manufacturers: {self.df['bit_manufacturer'].nunique()}")
        
        return output_path
    
    def run_enhanced_cleanup(self):
        """Run the complete enhanced cleanup process"""
        print("🏢 ENHANCED BIT MANUFACTURER CONSOLIDATION")
        print("=" * 50)
        
        # Load data
        filename = self.load_latest_dataset()
        
        if self.df is None:
            logger.error("❌ Failed to load dataset")
            return
        
        # Show current state
        current_counts = self.df['bit_manufacturer'].value_counts()
        print(f"\nCurrent manufacturers: {len(current_counts)}")
        
        # Apply enhanced cleanup
        changes_made = self.apply_enhanced_cleanup()
        
        if changes_made > 0:
            # Analyze results
            self.analyze_final_results()
            
            # Save final dataset
            final_file = self.save_final_dataset()
            
            print(f"\n✅ ENHANCED CONSOLIDATION COMPLETE!")
            if final_file:
                print(f"   Final dataset: {final_file.name}")
            print(f"   Records updated: {changes_made:,}")
        else:
            logger.info("ℹ️  No additional consolidation needed")

def main():
    """Main execution function"""
    cleanup = EnhancedManufacturerCleanup()
    cleanup.run_enhanced_cleanup()

if __name__ == "__main__":
    main()
