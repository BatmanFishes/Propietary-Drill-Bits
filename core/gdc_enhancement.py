"""
GDC Database Enhancement Module
Enhances integrated drilling data with authoritative GDC license numbers and UWI values.

This module processes ALL records, matching on trimmed license numbers and updating:
- license_number with GDC's WELL_NUM (preserving leading zeros)
- uwi_number with GDC's UWI
- uwi_formatted with GDC's GSL_UWID

This replaces the old safe/standard lookup approaches with a comprehensive solution.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import oracledb
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)

class GDCEnhancer:
    """
    Comprehensive GDC database enhancement for drilling data
    Processes all records to standardize license numbers and enhance UWI data
    """
    
    def __init__(self):
        self.connection_params = {
            'host': 'WC-CGY-ORAP01',
            'port': 1521,
            'service': 'PRD1',
            'user': 'synergyro',
            'password': 'synergyro',
            'schema': 'GDC'
        }
        self.connection = None
        
    def connect(self) -> bool:
        """Establish connection to Oracle database"""
        try:
            dsn = f"{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['service']}"
            self.connection = oracledb.connect(
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                dsn=dsn
            )
            logger.info("✅ Connected to GDC Oracle database for enhancement")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to GDC database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Disconnected from GDC database")
    
    def trim_leading_zeros(self, value) -> Optional[str]:
        """Trim leading zeros from license numbers for consistent matching"""
        if pd.isna(value) or value is None:
            return None
        
        # Convert to string and clean up
        str_value = str(value).strip()
        
        # If it's empty after stripping, return None
        if not str_value or str_value.lower() in ['nan', 'none', '']:
            return None
        
        # Remove decimal places if present
        if '.' in str_value and str_value.replace('.', '').replace('-', '').isdigit():
            try:
                float_val = float(str_value)
                if float_val == int(float_val):  # No fractional part
                    str_value = str(int(float_val))
            except (ValueError, OverflowError):
                pass
        
        # Trim leading zeros but preserve the string
        if str_value.isdigit():
            trimmed = str_value.lstrip('0')
            return trimmed if trimmed else '0'
        
        return str_value
    
    def derive_province_from_longitude(self, longitude) -> Optional[str]:
        """
        Derive province from longitude using AB/BC border at -120°W
        
        Args:
            longitude: Longitude value (can be positive or negative)
            
        Returns:
            'AB' for Alberta, 'BC' for British Columbia, None if cannot determine
        """
        if pd.isna(longitude) or longitude is None:
            return None
            
        try:
            # Convert to float and handle both positive and negative values
            lng = float(longitude)
            
            # If positive, assume it should be negative (common data entry issue)
            if lng > 0:
                lng = -lng
                
            # AB/BC border is at -120°W longitude
            # West of -120° = BC, East of -120° = AB
            if lng <= -120.0:
                return 'BC'
            elif lng > -120.0:
                return 'AB'
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    
    def build_gdc_lookup_table(self) -> pd.DataFrame:
        """
        Build a comprehensive lookup table from GDC database
        Returns DataFrame with trimmed license numbers for matching
        """
        logger.info("🔄 Building GDC license/UWI lookup table...")
        
        query = """
        SELECT WELL_NUM,
               UWI,
               GSL_UWID,
               WELL_NAME,
               PROVINCE_STATE,
               SURFACE_LATITUDE,
               SURFACE_LONGITUDE,
               BOTTOM_HOLE_LATITUDE,
               BOTTOM_HOLE_LONGITUDE,
               DRILL_TD,
               FINAL_DRILL_DATE,
               FINAL_TD,
               GSL_DAYS_ON,
               MAX_TVD,
               PROFILE_TYPE,
               RIG_RELEASE_DATE,
               SPUD_DATE
        FROM GDC.WELL 
        WHERE WELL_NUM IS NOT NULL
        AND PROVINCE_STATE IN ('AB', 'BC')
        """
        
        try:
            gdc_df = pd.read_sql(query, self.connection)
            logger.info(f"📊 Retrieved {len(gdc_df)} records from GDC database")
            
            # Create trimmed license numbers for matching
            gdc_df['WELL_NUM_TRIMMED'] = gdc_df['WELL_NUM'].apply(self.trim_leading_zeros)
            
            # Remove records where trimmed license is None/empty
            gdc_df = gdc_df[gdc_df['WELL_NUM_TRIMMED'].notna()].copy()
            
            logger.info(f"📊 {len(gdc_df)} records with valid license numbers for matching")
            
            # Check for duplicates and deduplicate if necessary
            duplicate_count = gdc_df['WELL_NUM_TRIMMED'].duplicated().sum()
            if duplicate_count > 0:
                logger.warning(f"⚠️  Found {duplicate_count} duplicate license numbers in GDC - deduplicating...")
                
                # Show some examples of duplicates before deduplication
                dup_examples = gdc_df[gdc_df['WELL_NUM_TRIMMED'].duplicated(keep=False)]['WELL_NUM_TRIMMED'].unique()[:3]
                logger.warning(f"   Example duplicate licenses: {list(dup_examples)}")
                
                # Smart deduplication: create sorting keys to prefer BC UWIs
                def get_uwi_priority(uwi):
                    if pd.isna(uwi):
                        return 9999  # Lowest priority for null UWIs
                    uwi_str = str(uwi)
                    
                    # Modern BC UWIs (200, 201, 202, etc.) - highest priority
                    if uwi_str.startswith('200'):
                        return 1
                    elif uwi_str.startswith('201'):
                        return 2  
                    elif uwi_str.startswith('202'):
                        return 3
                    elif uwi_str.startswith('20') and len(uwi_str) == 16:
                        return 4  # Other 20x BC UWIs
                    
                    # Old BC format (1BC, 2BC, etc.) - medium priority
                    elif 'BC' in uwi_str:
                        return 5
                    
                    # Alberta UWIs starting with 1 - lower priority
                    elif uwi_str.startswith('1') and len(uwi_str) == 16:
                        return 6
                    
                    # Everything else - lowest priority
                    else:
                        return 7
                
                gdc_df['uwi_priority'] = gdc_df['UWI'].apply(get_uwi_priority)
                gdc_df = gdc_df.sort_values(['WELL_NUM_TRIMMED', 'uwi_priority', 'UWI']).drop_duplicates(subset=['WELL_NUM_TRIMMED'], keep='first')
                gdc_df = gdc_df.drop(columns=['uwi_priority'])
                logger.info(f"📊 After smart deduplication: {len(gdc_df)} unique license numbers")
                
                # Validate deduplication worked
                remaining_duplicates = gdc_df['WELL_NUM_TRIMMED'].duplicated().sum()
                if remaining_duplicates > 0:
                    logger.error(f"❌ DEDUPLICATION FAILED: Still have {remaining_duplicates} duplicates!")
                    dup_examples = gdc_df[gdc_df['WELL_NUM_TRIMMED'].duplicated(keep=False)]['WELL_NUM_TRIMMED'].unique()[:3]
                    logger.error(f"   Examples: {list(dup_examples)}")
                    return pd.DataFrame()  # Return empty to prevent merge issues
                else:
                    logger.info("✅ Deduplication successful - no remaining duplicates")
            
            return gdc_df
            
        except Exception as e:
            logger.error(f"❌ Error building GDC lookup table: {e}")
            return pd.DataFrame()
    
    def enhance_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Enhance integrated data with GDC license numbers and UWI values
        
        Returns:
            Tuple of (enhanced_dataframe, enhancement_stats)
        """
        if not self.connect():
            return df, {'error': 'Failed to connect to GDC database'}
        
        try:
            # Build GDC lookup table
            gdc_lookup = self.build_gdc_lookup_table()
            if gdc_lookup.empty:
                logger.error("❌ No GDC lookup data available")
                return df, {'error': 'No GDC lookup data available'}
            
            # Create enhanced copy of input data
            enhanced_df = df.copy()
            
            # Track enhancement statistics
            stats = {
                'total_records': len(enhanced_df),
                'records_with_license': 0,
                'gdc_matches_found': 0,
                'license_enhanced': 0,
                'uwi_enhanced': 0,
                'uwi_formatted_enhanced': 0,
                'province_enhanced': 0,
                'province_derived': 0,
                'unmatched_licenses': []
            }
            
            # Derive province from longitude if not already present
            if 'state_province' not in enhanced_df.columns or enhanced_df['state_province'].isna().all():
                logger.info("🌍 Deriving province from longitude coordinates...")
                
                # Add state_province column if it doesn't exist
                if 'state_province' not in enhanced_df.columns:
                    enhanced_df['state_province'] = None
                
                # Derive province for records missing it
                missing_province = enhanced_df['state_province'].isna()
                if missing_province.any() and 'longitude' in enhanced_df.columns:
                    enhanced_df.loc[missing_province, 'state_province'] = enhanced_df.loc[missing_province, 'longitude'].apply(
                        self.derive_province_from_longitude
                    )
                    
                    derived_count = enhanced_df.loc[missing_province, 'state_province'].notna().sum()
                    stats['province_derived'] = derived_count
                    logger.info(f"   🎯 Derived province for {derived_count} records from longitude")
            
            # Add trimmed license numbers for matching
            enhanced_df['license_number_trimmed'] = enhanced_df['license_number'].apply(self.trim_leading_zeros)
            
            # Check for duplicates in source data
            source_duplicates = enhanced_df['license_number_trimmed'].duplicated().sum()
            if source_duplicates > 0:
                logger.info(f"ℹ️  Found {source_duplicates} duplicate license numbers in source data (normal for multiple bit runs per well)")
                
                # This is actually normal - same well can have multiple bit runs
                # Show some examples for information
                dup_licenses = enhanced_df[enhanced_df['license_number_trimmed'].duplicated(keep=False)]['license_number_trimmed'].unique()[:3]
                logger.info(f"   Example licenses with multiple runs: {list(dup_licenses)}")
            else:
                logger.info("ℹ️  All license numbers in source data are unique")
            
            # Count records with license numbers
            stats['records_with_license'] = enhanced_df['license_number_trimmed'].notna().sum()
            
            logger.info(f"🔄 Enhancing {stats['total_records']} records ({stats['records_with_license']} with license numbers)...")
            
            # Check GDC lookup for duplicates before merge - CRITICAL for preventing Type 2 duplicates
            gdc_duplicates = gdc_lookup['WELL_NUM_TRIMMED'].duplicated().sum()
            if gdc_duplicates > 0:
                logger.error(f"❌ CRITICAL: Found {gdc_duplicates} duplicate trimmed license numbers in GDC lookup!")
                logger.error("   This WILL cause Type 2 duplicates (artificial row creation)!")
                
                # Show the duplicates
                dup_gdc_licenses = gdc_lookup[gdc_lookup['WELL_NUM_TRIMMED'].duplicated(keep=False)]['WELL_NUM_TRIMMED'].unique()[:5]
                logger.error(f"   Example duplicate GDC licenses: {list(dup_gdc_licenses)}")
                
                # Show detailed info about the first duplicate
                if len(dup_gdc_licenses) > 0:
                    first_dup = dup_gdc_licenses[0]
                    dup_records = gdc_lookup[gdc_lookup['WELL_NUM_TRIMMED'] == first_dup][['WELL_NUM', 'WELL_NUM_TRIMMED', 'UWI', 'PROVINCE_STATE']]
                    logger.error(f"   Example duplicate records for license {first_dup}:")
                    for _, row in dup_records.iterrows():
                        logger.error(f"     {row['WELL_NUM']} -> {row['WELL_NUM_TRIMMED']} | UWI: {row['UWI']} | Province: {row['PROVINCE_STATE']}")
                
                # This should not happen - abort merge to prevent data corruption
                return df, {'error': f'GDC lookup table has {gdc_duplicates} duplicate license numbers - this will create artificial duplicate rows (Type 2)'}
            else:
                logger.info("✅ GDC lookup table verified - no duplicates that could cause Type 2 duplicate rows")
            
            # Merge with GDC data on trimmed license numbers
            merged_df = enhanced_df.merge(
                gdc_lookup,
                left_on='license_number_trimmed',
                right_on='WELL_NUM_TRIMMED',
                how='left',
                suffixes=('', '_GDC')
            )
            
            # Validate merge didn't create Type 2 duplicates (artificial row creation)
            # Note: Row count MUST remain the same - a left merge with deduplicated right table
            # should NEVER increase row count
            if len(merged_df) != len(enhanced_df):
                logger.error(f"❌ TYPE 2 DUPLICATE ERROR: Row count changed from {len(enhanced_df)} to {len(merged_df)}")
                logger.error("   This means we artificially created duplicate rows!")
                logger.error("   Type 1 duplicates (multiple bit runs per well) are preserved.")
                logger.error("   Type 2 duplicates (artificial row creation) are PREVENTED.")
                
                # Diagnostic information
                logger.error(f"   Input rows: {len(enhanced_df)}")
                logger.error(f"   Output rows: {len(merged_df)}")
                logger.error(f"   Artificial rows created: {len(merged_df) - len(enhanced_df)}")
                
                logger.error("   This indicates our GDC deduplication failed or there's a merge bug.")
                logger.error("   Aborting to prevent data corruption.")
                
                # Fall back to original data to prevent Type 2 duplicates
                return df, {'error': 'Merge created Type 2 duplicates (artificial rows) - prevented data corruption by aborting'}
            else:
                logger.info("✅ Merge validation passed - no Type 2 duplicates created")
                logger.info("   Type 1 duplicates (multiple bit runs per well) preserved as data")
            
            # Count matches
            gdc_matches = merged_df['WELL_NUM'].notna()
            stats['gdc_matches_found'] = gdc_matches.sum()
            
            logger.info(f"✅ Found {stats['gdc_matches_found']} GDC matches")
            
            # Update license numbers with GDC's WELL_NUM (preserving leading zeros)
            license_updates = gdc_matches & (merged_df['WELL_NUM'] != merged_df['license_number'])
            merged_df.loc[license_updates, 'license_number'] = merged_df.loc[license_updates, 'WELL_NUM']
            stats['license_enhanced'] = license_updates.sum()
            
            # Update UWI number with GDC's UWI
            uwi_updates = gdc_matches & (
                merged_df['UWI'].notna() & 
                ((merged_df['uwi_number'].isna()) | (merged_df['UWI'] != merged_df['uwi_number']))
            )
            merged_df.loc[uwi_updates, 'uwi_number'] = merged_df.loc[uwi_updates, 'UWI']
            stats['uwi_enhanced'] = uwi_updates.sum()
            
            # Update UWI formatted with GDC's GSL_UWID
            if 'uwi_formatted' in merged_df.columns:
                uwi_formatted_updates = gdc_matches & (
                    merged_df['GSL_UWID'].notna() & 
                    ((merged_df['uwi_formatted'].isna()) | (merged_df['GSL_UWID'] != merged_df['uwi_formatted']))
                )
                merged_df.loc[uwi_formatted_updates, 'uwi_formatted'] = merged_df.loc[uwi_formatted_updates, 'GSL_UWID']
                stats['uwi_formatted_enhanced'] = uwi_formatted_updates.sum()
            else:
                # Create the column if it doesn't exist and populate with GDC data
                merged_df['uwi_formatted'] = None
                uwi_formatted_updates = gdc_matches & merged_df['GSL_UWID'].notna()
                merged_df.loc[uwi_formatted_updates, 'uwi_formatted'] = merged_df.loc[uwi_formatted_updates, 'GSL_UWID']
                stats['uwi_formatted_enhanced'] = uwi_formatted_updates.sum()
            
            # Update province/state with GDC's PROVINCE_STATE
            province_updates = gdc_matches & (
                merged_df['PROVINCE_STATE'].notna() & 
                ((merged_df['state_province'].isna()) | (merged_df['PROVINCE_STATE'] != merged_df['state_province']))
            )
            merged_df.loc[province_updates, 'state_province'] = merged_df.loc[province_updates, 'PROVINCE_STATE']
            stats['province_enhanced'] = province_updates.sum()
            
            # Update GDC well data fields with prefixed names
            gdc_field_mappings = {
                'BOTTOM_HOLE_LATITUDE': 'gdc_bottom_hole_latitude',
                'BOTTOM_HOLE_LONGITUDE': 'gdc_bottom_hole_longitude', 
                'DRILL_TD': 'gdc_drill_td',
                'FINAL_DRILL_DATE': 'gdc_final_drill_date',
                'FINAL_TD': 'gdc_final_td',
                'GSL_DAYS_ON': 'gdc_gsl_days_on',
                'MAX_TVD': 'gdc_max_tvd',
                'PROFILE_TYPE': 'gdc_profile_type',
                'RIG_RELEASE_DATE': 'gdc_rig_release_date',
                'SPUD_DATE': 'gdc_spud_date',
                'SURFACE_LATITUDE': 'gdc_surface_latitude',
                'SURFACE_LONGITUDE': 'gdc_surface_longitude'
            }
            
            gdc_well_updates = 0
            for gdc_col, standard_col in gdc_field_mappings.items():
                if gdc_col in merged_df.columns:
                    # Add the column if it doesn't exist
                    if standard_col not in merged_df.columns:
                        merged_df[standard_col] = None
                    
                    # Update values where GDC has data and we either have no data or different data
                    field_updates = gdc_matches & merged_df[gdc_col].notna()
                    merged_df.loc[field_updates, standard_col] = merged_df.loc[field_updates, gdc_col]
                    gdc_well_updates += field_updates.sum()
            
            stats['gdc_well_fields_enhanced'] = gdc_well_updates
            
            # Track unmatched license numbers
            has_license_no_match = (
                enhanced_df['license_number_trimmed'].notna() & 
                merged_df['WELL_NUM'].isna()
            )
            if has_license_no_match.any():
                unmatched = enhanced_df.loc[has_license_no_match, 'license_number'].dropna().unique()
                stats['unmatched_licenses'] = unmatched.tolist()[:20]  # Limit to first 20 for reporting
            
            # Clean up temporary columns
            columns_to_drop = [
                'license_number_trimmed', 'WELL_NUM', 'UWI', 'GSL_UWID', 
                'WELL_NAME_GDC', 'PROVINCE_STATE', 'WELL_NUM_TRIMMED',
                # Original GDC columns that are now mapped to prefixed versions
                'BOTTOM_HOLE_LATITUDE', 'BOTTOM_HOLE_LONGITUDE',
                'DRILL_TD', 'FINAL_DRILL_DATE', 'FINAL_TD', 'GSL_DAYS_ON',
                'MAX_TVD', 'PROFILE_TYPE', 'RIG_RELEASE_DATE',
                'SPUD_DATE', 'SURFACE_LATITUDE', 'SURFACE_LONGITUDE'
            ]
            enhanced_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])
            
            # Log enhancement results
            logger.info(f"📊 GDC Enhancement Results:")
            logger.info(f"   🔢 Total records: {stats['total_records']}")
            logger.info(f"   📋 Records with licenses: {stats['records_with_license']}")
            logger.info(f"   ✅ GDC matches found: {stats['gdc_matches_found']}")
            logger.info(f"   🔄 License numbers enhanced: {stats['license_enhanced']}")
            logger.info(f"   🆔 UWI numbers enhanced: {stats['uwi_enhanced']}")
            logger.info(f"   📝 UWI formatted enhanced: {stats['uwi_formatted_enhanced']}")
            logger.info(f"   🌍 Province/state enhanced: {stats['province_enhanced']}")
            logger.info(f"   🏗️  GDC well data fields enhanced: {stats['gdc_well_fields_enhanced']}")
            logger.info(f"   🎯 Province derived from longitude: {stats['province_derived']}")
            logger.info(f"   ❓ Unmatched licenses: {len(stats.get('unmatched_licenses', []))}")
            
            return enhanced_df, stats
            
        except Exception as e:
            logger.error(f"❌ Error during GDC enhancement: {e}")
            return df, {'error': str(e)}
        
        finally:
            self.disconnect()
    
    def generate_enhancement_report(self, stats: Dict, output_dir: Path) -> Optional[Path]:
        """Generate a detailed enhancement report"""
        if 'error' in stats:
            logger.error(f"Cannot generate report due to error: {stats['error']}")
            return None
        
        try:
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            report_file = output_dir / f"GDC_Enhancement_Report_{timestamp}.txt"
            
            with open(report_file, 'w') as f:
                f.write("GDC DATABASE ENHANCEMENT REPORT\n")
                f.write("=" * 50 + "\n")
                f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("ENHANCEMENT SUMMARY\n")
                f.write("-" * 20 + "\n")
                f.write(f"Total records processed: {stats['total_records']:,}\n")
                f.write(f"Records with license numbers: {stats['records_with_license']:,}\n")
                f.write(f"GDC matches found: {stats['gdc_matches_found']:,}\n")
                f.write(f"Match rate: {stats['gdc_matches_found']/max(stats['records_with_license'], 1)*100:.1f}%\n\n")
                
                f.write("FIELD ENHANCEMENTS\n")
                f.write("-" * 18 + "\n")
                f.write(f"License numbers standardized: {stats['license_enhanced']:,}\n")
                f.write(f"UWI numbers added/updated: {stats['uwi_enhanced']:,}\n")
                f.write(f"UWI formatted added/updated: {stats['uwi_formatted_enhanced']:,}\n")
                f.write(f"Province/state enhanced from GDC: {stats['province_enhanced']:,}\n")
                f.write(f"Province derived from longitude: {stats['province_derived']:,}\n\n")
                
                if stats['unmatched_licenses']:
                    f.write("UNMATCHED LICENSE NUMBERS (first 20)\n")
                    f.write("-" * 35 + "\n")
                    for license_num in stats['unmatched_licenses']:
                        f.write(f"  {license_num}\n")
                    f.write(f"\n({len(stats['unmatched_licenses'])} total unmatched shown)\n")
            
            logger.info(f"📄 Enhancement report saved: {report_file.name}")
            return report_file
            
        except Exception as e:
            logger.error(f"❌ Error generating enhancement report: {e}")
            return None

def enhance_with_gdc(df: pd.DataFrame, output_dir: Path) -> Tuple[pd.DataFrame, Dict]:
    """
    Convenience function to enhance dataframe with GDC data
    
    Args:
        df: DataFrame to enhance
        output_dir: Directory for saving reports
        
    Returns:
        Tuple of (enhanced_dataframe, enhancement_stats)
    """
    enhancer = GDCEnhancer()
    enhanced_df, stats = enhancer.enhance_data(df)
    
    # Generate report if enhancement was successful
    if 'error' not in stats:
        enhancer.generate_enhancement_report(stats, output_dir)
    
    return enhanced_df, stats
