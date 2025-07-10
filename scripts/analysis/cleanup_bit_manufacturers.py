#!/usr/bin/env python3
"""
Bit Manufacturer Name Cleanup Script
Standardizes bit manufacturer names and consolidates related companies
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ManufacturerCleanup:
    def __init__(self):
        self.output_dir = Path("Output")
        self.df = None
        self.manufacturer_mapping = {}
        self.cleanup_stats = {}
        
    def load_latest_dataset(self):
        """Load the most recent integrated dataset"""
        # Find the most recent SafeUpdated file, then most recent regular file
        safe_files = list(self.output_dir.glob("Integrated_BitData_SafeUpdated_*.xlsx"))
        regular_files = list(self.output_dir.glob("Integrated_BitData_*.xlsx"))
        
        # Filter out SafeUpdated from regular files
        regular_files = [f for f in regular_files if "SafeUpdated" not in f.name]
        
        if safe_files:
            latest_file = max(safe_files, key=lambda x: x.stat().st_mtime)
        elif regular_files:
            latest_file = max(regular_files, key=lambda x: x.stat().st_mtime)
        else:
            raise FileNotFoundError("No integrated data files found")
        
        logger.info(f"📊 Loading dataset: {latest_file.name}")
        self.df = pd.read_excel(latest_file)
        logger.info(f"📈 Total records loaded: {len(self.df):,}")
        
        return latest_file.name
    
    def analyze_current_manufacturers(self):
        """Analyze current manufacturer names in the dataset"""
        if self.df is None or 'bit_manufacturer' not in self.df.columns:
            logger.error("❌ No dataset loaded or bit_manufacturer column missing")
            return None
        
        # Get manufacturer counts
        manufacturer_counts = self.df['bit_manufacturer'].value_counts()
        
        print(f"\n📊 CURRENT BIT MANUFACTURERS ({len(manufacturer_counts)} unique names):")
        print("=" * 70)
        
        for i, (manufacturer, count) in enumerate(manufacturer_counts.items(), 1):
            percentage = (count / len(self.df)) * 100
            print(f"  {i:2d}. {manufacturer:<30} - {count:,} records ({percentage:.1f}%)")
        
        return manufacturer_counts
    
    def create_manufacturer_mapping(self):
        """Create standardized manufacturer name mapping"""
        
        # Company consolidation rules as specified
        self.manufacturer_mapping = {
            # NOV and Reed Hycalog are the same company -> Reed
            'NOV': 'Reed',
            'Reed Hycalog': 'Reed',
            'Reed': 'Reed',
            'REED': 'Reed',
            'reed': 'Reed',
            'NOV/Reed': 'Reed',
            'Reed/NOV': 'Reed',
            
            # Schlumberger and Smith are the same source -> Smith
            'Schlumberger': 'Smith',
            'Smith': 'Smith',
            'SMITH': 'Smith',
            'smith': 'Smith',
            'Smith Bits': 'Smith',
            'Smith International': 'Smith',
            'Schlumberger/Smith': 'Smith',
            'Smith/Schlumberger': 'Smith',
            
            # Halliburton and Security are the same -> Security
            'Halliburton': 'Security',
            'Security': 'Security',
            'SECURITY': 'Security',
            'security': 'Security',
            'Security DBS': 'Security',
            'Halliburton/Security': 'Security',
            'Security/Halliburton': 'Security',
            
            # Other common standardizations
            'Baker Hughes': 'Baker Hughes',
            'BAKER HUGHES': 'Baker Hughes',
            'baker hughes': 'Baker Hughes',
            'BHI': 'Baker Hughes',
            'Hughes': 'Baker Hughes',
            
            'Ulterra': 'Ulterra',
            'ULTERRA': 'Ulterra',
            'ulterra': 'Ulterra',
            
            'Varel': 'Varel',
            'VAREL': 'Varel',
            'varel': 'Varel',
            
            'Kingdream': 'Kingdream',
            'KINGDREAM': 'Kingdream',
            'kingdream': 'Kingdream',
            
            'Bit Brokers': 'Bit Brokers',
            'BIT BROKERS': 'Bit Brokers',
            'bit brokers': 'Bit Brokers',
            
            'National Oilwell': 'Reed',  # NOV subsidiary
            'National': 'Reed',
            
            # Handle null/missing values
            None: 'Unknown',
            'Unknown': 'Unknown',
            'UNKNOWN': 'Unknown',
            '': 'Unknown',
            np.nan: 'Unknown'
        }
        
        logger.info(f"📋 Created manufacturer mapping with {len(self.manufacturer_mapping)} entries")
        
        # Show the consolidation plan
        print(f"\n🔄 MANUFACTURER CONSOLIDATION PLAN:")
        print("=" * 50)
        
        consolidations = {}
        for original, standardized in self.manufacturer_mapping.items():
            if standardized not in consolidations:
                consolidations[standardized] = []
            consolidations[standardized].append(str(original))
        
        for standard_name, variations in consolidations.items():
            if len(variations) > 1:
                print(f"  {standard_name}: {', '.join(variations)}")
        
        return self.manufacturer_mapping
    
    def apply_manufacturer_cleanup(self):
        """Apply manufacturer name standardization"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        if 'bit_manufacturer' not in self.df.columns:
            logger.error("❌ bit_manufacturer column not found")
            return
        
        # Create backup of original values
        self.df['bit_manufacturer_original'] = self.df['bit_manufacturer'].copy()
        
        # Track changes
        changes_made = 0
        unchanged_count = 0
        
        # Apply mapping
        for idx, original_value in enumerate(self.df['bit_manufacturer']):
            # Handle NaN values
            if pd.isna(original_value):
                standardized = 'Unknown'
            else:
                original_str = str(original_value).strip()
                standardized = self.manufacturer_mapping.get(original_str, original_str)
            
            if str(standardized) != str(original_value):
                self.df.at[idx, 'bit_manufacturer'] = standardized
                changes_made += 1
            else:
                unchanged_count += 1
        
        logger.info(f"✅ Applied manufacturer cleanup: {changes_made:,} records changed, {unchanged_count:,} unchanged")
        
        # Generate cleanup statistics
        self.cleanup_stats = {
            'total_records': len(self.df),
            'records_changed': changes_made,
            'records_unchanged': unchanged_count,
            'original_manufacturers': self.df['bit_manufacturer_original'].nunique(),
            'standardized_manufacturers': self.df['bit_manufacturer'].nunique()
        }
        
        return changes_made
    
    def analyze_cleanup_results(self):
        """Analyze the results of manufacturer cleanup"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        # Show before/after comparison
        original_counts = self.df['bit_manufacturer_original'].value_counts()
        standardized_counts = self.df['bit_manufacturer'].value_counts()
        
        print(f"\n📊 CLEANUP RESULTS SUMMARY:")
        print("=" * 40)
        print(f"  Original manufacturers: {len(original_counts)}")
        print(f"  Standardized manufacturers: {len(standardized_counts)}")
        print(f"  Reduction: {len(original_counts) - len(standardized_counts)} fewer unique names")
        print(f"  Records changed: {self.cleanup_stats['records_changed']:,}")
        
        print(f"\n📊 TOP STANDARDIZED MANUFACTURERS:")
        print("=" * 50)
        for i, (manufacturer, count) in enumerate(standardized_counts.head(10).items(), 1):
            percentage = (count / len(self.df)) * 100
            print(f"  {i:2d}. {manufacturer:<20} - {count:,} records ({percentage:.1f}%)")
        
        # Show consolidation impact
        print(f"\n🔄 CONSOLIDATION IMPACT:")
        print("=" * 30)
        
        major_consolidations = {
            'Reed': ['NOV', 'Reed Hycalog', 'Reed', 'National Oilwell', 'National'],
            'Smith': ['Schlumberger', 'Smith', 'Smith Bits', 'Smith International'],
            'Security': ['Halliburton', 'Security', 'Security DBS'],
            'Baker Hughes': ['Baker Hughes', 'BHI', 'Hughes']
        }
        
        for standard_name, variations in major_consolidations.items():
            total_records = 0
            for variation in variations:
                count = original_counts.get(variation, 0)
                total_records += count
            
            if total_records > 0:
                print(f"  {standard_name}: {total_records:,} total records consolidated")
    
    def save_cleaned_dataset(self):
        """Save the cleaned dataset with standardized manufacturer names"""
        if self.df is None:
            logger.error("❌ No dataset to save")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"Integrated_BitData_CleanedManufacturers_{timestamp}.xlsx"
        output_path = self.output_dir / output_filename
        
        # Save the cleaned dataset
        logger.info(f"💾 Saving cleaned dataset: {output_filename}")
        self.df.to_excel(output_path, index=False)
        
        print(f"\n💾 CLEANED DATASET SAVED:")
        print(f"   File: {output_filename}")
        print(f"   Records: {len(self.df):,}")
        print(f"   Original manufacturers: {self.cleanup_stats['original_manufacturers']}")
        print(f"   Standardized manufacturers: {self.cleanup_stats['standardized_manufacturers']}")
        
        return output_path
    
    def generate_cleanup_report(self):
        """Generate detailed cleanup report"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"Manufacturer_Cleanup_Report_{timestamp}.xlsx"
        
        logger.info(f"📊 Generating manufacturer cleanup report: {report_file.name}")
        
        with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = [{
                'Metric': 'Total Records',
                'Value': self.cleanup_stats['total_records']
            }, {
                'Metric': 'Records Changed',
                'Value': self.cleanup_stats['records_changed']
            }, {
                'Metric': 'Records Unchanged',
                'Value': self.cleanup_stats['records_unchanged']
            }, {
                'Metric': 'Original Unique Manufacturers',
                'Value': self.cleanup_stats['original_manufacturers']
            }, {
                'Metric': 'Standardized Unique Manufacturers',
                'Value': self.cleanup_stats['standardized_manufacturers']
            }, {
                'Metric': 'Reduction in Unique Names',
                'Value': self.cleanup_stats['original_manufacturers'] - self.cleanup_stats['standardized_manufacturers']
            }]
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Original manufacturers
            original_counts = self.df['bit_manufacturer_original'].value_counts()
            original_df = pd.DataFrame({
                'Original_Manufacturer': original_counts.index,
                'Record_Count': original_counts.values,
                'Percentage': (original_counts.values.astype(float) / len(self.df) * 100).round(2)
            })
            original_df.to_excel(writer, sheet_name='Original_Manufacturers', index=False)
            
            # Standardized manufacturers
            standardized_counts = self.df['bit_manufacturer'].value_counts()
            standardized_df = pd.DataFrame({
                'Standardized_Manufacturer': standardized_counts.index,
                'Record_Count': standardized_counts.values,
                'Percentage': (standardized_counts.values.astype(float) / len(self.df) * 100).round(2)
            })
            standardized_df.to_excel(writer, sheet_name='Standardized_Manufacturers', index=False)
            
            # Mapping table
            mapping_df = pd.DataFrame([
                {'Original_Name': k, 'Standardized_Name': v} 
                for k, v in self.manufacturer_mapping.items()
            ])
            mapping_df.to_excel(writer, sheet_name='Mapping_Rules', index=False)
            
            # Changed records sample
            changed_records = self.df[
                self.df['bit_manufacturer'] != self.df['bit_manufacturer_original']
            ][['well_name', 'bit_manufacturer_original', 'bit_manufacturer', 'data_source']].head(100)
            
            changed_records.to_excel(writer, sheet_name='Sample_Changes', index=False)
        
        logger.info(f"✅ Cleanup report saved: {report_file.name}")
        return report_file
    
    def run_full_cleanup(self):
        """Run complete manufacturer cleanup process"""
        print("🧹 BIT MANUFACTURER CLEANUP")
        print("=" * 40)
        
        # Load data
        filename = self.load_latest_dataset()
        
        # Analyze current state
        original_manufacturers = self.analyze_current_manufacturers()
        
        if original_manufacturers is None:
            logger.error("❌ Cannot proceed without manufacturer data")
            return
        
        # Create mapping
        self.create_manufacturer_mapping()
        
        # Apply cleanup
        changes_made = self.apply_manufacturer_cleanup()
        
        if changes_made is not None and changes_made > 0:
            # Analyze results
            self.analyze_cleanup_results()
            
            # Save cleaned dataset
            cleaned_file = self.save_cleaned_dataset()
            
            # Generate report
            report_file = self.generate_cleanup_report()
            
            print(f"\n✅ MANUFACTURER CLEANUP COMPLETE!")
            if cleaned_file:
                print(f"   Cleaned dataset: {cleaned_file.name}")
            if report_file:
                print(f"   Detailed report: {report_file.name}")
            print(f"   Changes applied: {changes_made:,} records")
        else:
            logger.info("ℹ️  No manufacturer names needed cleanup")

def main():
    """Main execution function"""
    cleanup = ManufacturerCleanup()
    cleanup.run_full_cleanup()

if __name__ == "__main__":
    main()
