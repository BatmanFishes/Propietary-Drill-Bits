#!/usr/bin/env python3
"""
Duplicate Bit Records Detection and Analysis
Comprehensive analysis of duplicate bit records in the integrated dataset
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuplicateAnalyzer:
    def __init__(self):
        self.output_dir = Path("Output")
        self.df = None
        self.duplicates_found = {}
        
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
    
    def analyze_potential_duplicate_fields(self):
        """Analyze which fields could be used to identify duplicates"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return []
        
        logger.info("🔍 Analyzing potential duplicate identification fields...")
        
        # Key fields that could indicate duplicates
        key_fields = [
            'well_name', 'license_number', 'uwi',
            'bit_manufacturer', 'bit_size_mm', 'bit_type', 'bit_model',
            'run_date', 'spud_date', 'start_depth', 'end_depth',
            'operator', 'field', 'data_source'
        ]
        
        available_fields = [field for field in key_fields if field in self.df.columns]
        
        print("\n📋 AVAILABLE FIELDS FOR DUPLICATE DETECTION:")
        print("=" * 50)
        for i, field in enumerate(available_fields, 1):
            unique_count = self.df[field].nunique()
            null_count = self.df[field].isnull().sum()
            print(f"  {i:2d}. {field:<20} - {unique_count:,} unique values, {null_count:,} nulls")
        
        return available_fields
    
    def detect_exact_duplicates(self):
        """Detect exact duplicate records (all fields identical)"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        logger.info("🔍 Checking for exact duplicate records...")
        
        # Find exact duplicates
        exact_duplicates = self.df[self.df.duplicated(keep=False)]
        
        if len(exact_duplicates) > 0:
            logger.warning(f"⚠️  Found {len(exact_duplicates)} exact duplicate records")
            self.duplicates_found['exact'] = exact_duplicates
            
            # Group by duplicate sets
            duplicate_groups = []
            for group_id, group in exact_duplicates.groupby(list(exact_duplicates.columns)):
                if len(group) > 1:
                    duplicate_groups.append(group)
            
            print(f"\n❌ EXACT DUPLICATES FOUND: {len(exact_duplicates)} records in {len(duplicate_groups)} groups")
        else:
            logger.info("✅ No exact duplicate records found")
            print("\n✅ EXACT DUPLICATES: None found")
    
    def detect_key_field_duplicates(self):
        """Detect duplicates based on key business fields"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        logger.info("🔍 Checking for duplicates based on key business fields...")
        
        # Define different duplicate detection strategies
        strategies = {
            'well_bit_run': ['well_name', 'bit_manufacturer', 'bit_size_mm', 'run_date'],
            'well_depth_range': ['well_name', 'start_depth', 'end_depth'],
            'license_bit_run': ['license_number', 'bit_manufacturer', 'bit_size_mm', 'run_date'],
            'well_bit_type': ['well_name', 'bit_manufacturer', 'bit_type', 'bit_size_mm'],
            'same_run_details': ['well_name', 'run_date', 'start_depth', 'end_depth']
        }
        
        for strategy_name, fields in strategies.items():
            # Check if all fields exist
            available_fields = [f for f in fields if f in self.df.columns]
            if len(available_fields) < len(fields):
                missing = [f for f in fields if f not in self.df.columns]
                logger.warning(f"⚠️  Strategy '{strategy_name}' missing fields: {missing}")
                continue
            
            # Remove records with null values in key fields
            df_clean = self.df.dropna(subset=available_fields)
            
            # Find duplicates
            duplicates = df_clean[df_clean.duplicated(subset=available_fields, keep=False)]
            
            if len(duplicates) > 0:
                duplicate_groups = duplicates.groupby(available_fields).size()
                group_count = len(duplicate_groups[duplicate_groups > 1])
                
                logger.warning(f"⚠️  Strategy '{strategy_name}': {len(duplicates)} potential duplicates in {group_count} groups")
                self.duplicates_found[strategy_name] = duplicates
                
                print(f"\n⚠️  POTENTIAL DUPLICATES - {strategy_name.upper()}:")
                print(f"    Records: {len(duplicates)}")
                print(f"    Groups: {group_count}")
                print(f"    Fields used: {', '.join(available_fields)}")
                
                # Show sample of largest duplicate group
                largest_group = duplicates.groupby(available_fields).size().idxmax()
                sample_group = duplicates.groupby(available_fields).get_group(largest_group)
                
                print(f"    Largest group ({len(sample_group)} records):")
                for field in available_fields:
                    print(f"      {field}: {sample_group[field].iloc[0]}")
            else:
                logger.info(f"✅ Strategy '{strategy_name}': No duplicates found")
    
    def analyze_near_duplicates(self):
        """Analyze potential near-duplicates (similar but not identical)"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        logger.info("🔍 Analyzing near-duplicate patterns...")
        
        # Look for records with same well but different data sources
        if 'well_name' in self.df.columns and 'data_source' in self.df.columns:
            well_source_counts = self.df.groupby('well_name')['data_source'].nunique()
            multi_source_wells = well_source_counts[well_source_counts > 1]
            
            if len(multi_source_wells) > 0:
                logger.info(f"📊 Found {len(multi_source_wells)} wells with data from multiple sources")
                
                print(f"\n📊 WELLS WITH MULTIPLE DATA SOURCES: {len(multi_source_wells)}")
                print("    (These might represent the same bit runs from different sources)")
                
                # Show details for some examples
                sample_wells = multi_source_wells.head(5).index
                for well in sample_wells:
                    well_records = self.df[self.df['well_name'] == well]
                    sources = well_records['data_source'].unique()
                    print(f"    • {well}: {len(well_records)} records from {sources}")
                
                self.duplicates_found['multi_source_wells'] = multi_source_wells
    
    def analyze_temporal_duplicates(self):
        """Analyze duplicates based on temporal proximity"""
        if self.df is None:
            logger.error("❌ No dataset loaded")
            return
        
        if 'run_date' not in self.df.columns or 'well_name' not in self.df.columns:
            logger.warning("⚠️  Cannot analyze temporal duplicates - missing run_date or well_name")
            return
        
        logger.info("🔍 Analyzing temporal duplicate patterns...")
        
        # Convert run_date to datetime
        df_temp = self.df.copy()
        df_temp['run_date'] = pd.to_datetime(df_temp['run_date'], errors='coerce')
        
        # Group by well and look for runs on same date
        same_date_runs = []
        for well, group in df_temp.groupby('well_name'):
            if len(group) > 1:
                date_counts = group['run_date'].value_counts()
                multi_run_dates = date_counts[date_counts > 1]
                
                if len(multi_run_dates) > 0:
                    for date, count in multi_run_dates.items():
                        same_date_runs.append({
                            'well_name': well,
                            'run_date': date,
                            'record_count': count
                        })
        
        if same_date_runs:
            logger.warning(f"⚠️  Found {len(same_date_runs)} date/well combinations with multiple records")
            print(f"\n⚠️  MULTIPLE RECORDS SAME DATE/WELL: {len(same_date_runs)} cases")
            
            # Show examples
            for i, case in enumerate(same_date_runs[:5], 1):
                print(f"    {i}. {case['well_name']} on {case['run_date'].strftime('%Y-%m-%d') if pd.notna(case['run_date']) else 'Unknown'}: {case['record_count']} records")
    
    def generate_duplicate_report(self):
        """Generate comprehensive duplicate analysis report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"Duplicate_Analysis_Report_{timestamp}.xlsx"
        
        logger.info(f"📊 Generating duplicate analysis report: {report_file.name}")
        
        with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = []
            total_potential_duplicates = 0
            
            for duplicate_type, data in self.duplicates_found.items():
                if isinstance(data, pd.DataFrame):
                    count = len(data)
                    total_potential_duplicates += count
                elif isinstance(data, pd.Series):
                    count = len(data)
                else:
                    count = len(data) if hasattr(data, '__len__') else 1
                
                summary_data.append({
                    'Duplicate_Type': duplicate_type,
                    'Records_Count': count,
                    'Description': self._get_duplicate_description(duplicate_type)
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Individual duplicate type sheets
            for duplicate_type, data in self.duplicates_found.items():
                sheet_name = duplicate_type.replace('_', ' ').title()[:31]  # Excel sheet name limit
                
                if isinstance(data, pd.DataFrame):
                    # Sort by the grouping fields if possible
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
                elif isinstance(data, pd.Series):
                    # Convert series to dataframe
                    df_out = pd.DataFrame({
                        'Well_Name': data.index,
                        'Source_Count': data.values
                    })
                    df_out.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Print summary
        print(f"\n📊 DUPLICATE ANALYSIS SUMMARY:")
        print("=" * 40)
        if self.df is not None:
            print(f"  Total records analyzed: {len(self.df):,}")
        print(f"  Duplicate types found: {len(self.duplicates_found)}")
        print(f"  Report saved: {report_file.name}")
        
        for duplicate_type, data in self.duplicates_found.items():
            if isinstance(data, (pd.DataFrame, pd.Series)):
                count = len(data)
                print(f"  • {duplicate_type}: {count:,} records")
        
        return report_file
    
    def _get_duplicate_description(self, duplicate_type):
        """Get description for duplicate type"""
        descriptions = {
            'exact': 'Completely identical records (all fields match)',
            'well_bit_run': 'Same well, bit manufacturer, size, and run date',
            'well_depth_range': 'Same well with overlapping depth ranges',
            'license_bit_run': 'Same license, bit manufacturer, size, and run date',
            'well_bit_type': 'Same well with identical bit specifications',
            'same_run_details': 'Same well with identical run timing and depths',
            'multi_source_wells': 'Wells appearing in multiple data sources'
        }
        return descriptions.get(duplicate_type, 'Custom duplicate detection criteria')
    
    def run_full_analysis(self):
        """Run complete duplicate analysis"""
        print("🔍 COMPREHENSIVE DUPLICATE ANALYSIS")
        print("=" * 50)
        
        # Load data
        filename = self.load_latest_dataset()
        
        # Analyze available fields
        available_fields = self.analyze_potential_duplicate_fields()
        
        # Run different duplicate detection methods
        self.detect_exact_duplicates()
        self.detect_key_field_duplicates()
        self.analyze_near_duplicates()
        self.analyze_temporal_duplicates()
        
        # Generate report
        if self.duplicates_found:
            report_file = self.generate_duplicate_report()
            print(f"\n✅ Analysis complete! Detailed report saved: {report_file.name}")
        else:
            print(f"\n✅ Analysis complete! No duplicates found in {filename}")
        
        return self.duplicates_found

def main():
    """Main execution function"""
    analyzer = DuplicateAnalyzer()
    duplicates = analyzer.run_full_analysis()
    
    if duplicates:
        print(f"\n⚠️  RECOMMENDATION: Review the generated report and consider:")
        print("   1. Removing exact duplicates")
        print("   2. Investigating potential business logic duplicates")
        print("   3. Validating multi-source records for actual duplication")
        print("   4. Updating data integration logic to prevent future duplicates")
    else:
        print(f"\n✅ EXCELLENT: No duplicate records detected!")

if __name__ == "__main__":
    main()
