"""
Safe Oracle GDC Well Table License Lookup
A more conservative approach to prevent false matches by requiring high-confidence unique identifiers
"""

import pandas as pd
import numpy as np
from pathlib import Path
import oracledb
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta
import re
from difflib import SequenceMatcher

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SafeGDCLicenseLookup:
    """
    Safe GDC Oracle database lookup with strict matching criteria to prevent false matches
    Only assigns license numbers when there is high confidence in well identification
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
            logger.info("✅ Successfully connected to Oracle GDC database")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Oracle database: {e}")
            return False
    
    def trim_leading_zeros(self, value) -> Optional[str]:
        """Trim leading zeros from license numbers and well numbers for consistent matching"""
        import pandas as pd
        
        if pd.isna(value) or value is None:
            return None
        
        # Convert to string and clean up
        str_value = str(value).strip()
        
        # If it's empty after stripping, return None
        if not str_value or str_value.lower() in ['nan', 'none', '']:
            return None
        
        # Remove decimal places if present (e.g., "497050.0" -> "497050")
        if '.' in str_value and str_value.replace('.', '').replace('-', '').isdigit():
            try:
                float_val = float(str_value)
                if float_val == int(float_val):  # No fractional part
                    str_value = str(int(float_val))
            except (ValueError, OverflowError):
                pass
        
        # Trim leading zeros but preserve the string (important for license numbers)
        # Only trim if the string is purely numeric
        if str_value.isdigit():
            # Keep at least one digit
            trimmed = str_value.lstrip('0')
            return trimmed if trimmed else '0'
        
        return str_value

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Disconnected from Oracle database")
    
    def explore_well_table_structure(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Explore the GDC.WELL table structure"""
        try:
            structure_query = """
            SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
            FROM ALL_TAB_COLUMNS 
            WHERE OWNER = 'GDC' 
            AND TABLE_NAME = 'WELL'
            ORDER BY COLUMN_ID
            """
            
            structure_df = pd.read_sql(structure_query, self.connection)
            
            sample_query = """
            SELECT * FROM (
                SELECT * FROM GDC.WELL 
                WHERE ROWNUM <= 5
            )
            """
            
            sample_df = pd.read_sql(sample_query, self.connection)
            
            return structure_df, sample_df
            
        except Exception as e:
            logger.error(f"❌ Error exploring table structure: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def find_matching_columns(self, structure_df: pd.DataFrame) -> Dict[str, str]:
        """Find matching columns in the GDC.WELL table"""
        column_patterns = {
            'license_number': ['WELL_NUM', 'WELL_GOVERNMENT_ID', 'LICENSE', 'LICENCE', 'WELL_ID'],
            'well_name': ['WELL_NAME', 'NAME'],
            'uwi': ['UWI', 'UNIQUE_WELL', 'IDENTIFIER'],
            'surface_latitude': ['SURFACE_LATITUDE', 'SURF_LAT', 'LAT'],
            'surface_longitude': ['SURFACE_LONGITUDE', 'SURF_LONG', 'SURF_LON', 'LONG', 'LON'],
            'spud_date': ['SPUD_DATE', 'SPUD'],
            'field': ['FIELD', 'FIELD_NAME'],
            'legal_location': ['LEGAL', 'LOCATION', 'LSD'],
            'section': ['SECTION', 'SEC'],
            'township': ['TOWNSHIP', 'TWP'],
            'range': ['RANGE', 'RGE'],
            'meridian': ['MERIDIAN', 'MER']
        }
        
        column_mapping = {}
        available_columns = structure_df['COLUMN_NAME'].str.upper().tolist()
        
        print(f"\n🔍 AVAILABLE GDC.WELL COLUMNS:")
        print("=" * 50)
        for col in available_columns:
            print(f"  {col}")
        
        print(f"\n🎯 SAFE MATCHING COLUMN MAPPINGS:")
        print("=" * 50)
        
        for purpose, patterns in column_patterns.items():
            found_column = None
            for pattern in patterns:
                matching_cols = [col for col in available_columns if pattern in col]
                if matching_cols:
                    found_column = matching_cols[0]
                    break
            
            if found_column:
                column_mapping[purpose] = found_column
                print(f"  {purpose:15s} -> {found_column}")
            else:
                print(f"  {purpose:15s} -> NOT FOUND")
        
        return column_mapping
    
    def load_missing_license_records(self) -> pd.DataFrame:
        """Load records that are missing license numbers"""
        output_dir = Path("Output")
        integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
        if not integrated_files:
            logger.error("No integrated data files found")
            return pd.DataFrame()
        
        latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"📊 Loading missing license data from: {latest_file.name}")
        
        df = pd.read_excel(latest_file)
        
        # Apply trimming to license numbers for consistent matching
        if 'license_number' in df.columns:
            df['license_number'] = df['license_number'].apply(self.trim_leading_zeros)
            df['license_number_trimmed'] = df['license_number']  # Keep trimmed version for matching
        
        missing_license = df[
            (df['data_source'] == 'ulterra') &
            ((df['license_number'].isna()) | (df['license_number'] == ''))
        ].copy()
        
        logger.info(f"🔍 Found {len(missing_license)} records missing license numbers")
        return missing_license
    
    def extract_legal_location_components(self, well_name: str) -> Dict[str, Optional[str]]:
        """
        Extract legal location components from well name
        Returns components that can be used for unique matching
        """
        if pd.isna(well_name) or not well_name:
            return {}
        
        # Pattern to match legal location: LSD-Section-Township-Range-Meridian
        # Examples: 6-6-59-24W5, 102/6-6-59-24W5, 1-01-059-24W5
        legal_pattern = r'(\d+/?)?(\d+)-(\d+)-(\d+)-(\d+)(W[45])'
        match = re.search(legal_pattern, well_name)
        
        if match:
            lsd = match.group(2) if match.group(1) else match.group(2)
            section = match.group(3)
            township = match.group(4)
            range_val = match.group(5)
            meridian = match.group(6)
            
            return {
                'lsd': lsd.zfill(2),  # Pad with zeros
                'section': section.zfill(2),
                'township': township.zfill(3),
                'range': range_val.zfill(2),
                'meridian': meridian,
                'full_legal': f"{lsd}-{section}-{township}-{range_val}{meridian}"
            }
        
        return {}
    
    def extract_uwi_component(self, well_name: str) -> Optional[str]:
        """Extract UWI component from well name if present"""
        if pd.isna(well_name) or not well_name:
            return None
        
        # Look for UWI pattern at the beginning or after spaces
        uwi_pattern = r'(\d{2}-\d{3}-\d{5}-W\d{3}-\d{2})'
        match = re.search(uwi_pattern, well_name)
        return match.group(1) if match else None
    
    def is_high_confidence_match(self, bit_record: pd.Series, gdc_record: pd.Series, 
                                match_type: str) -> Tuple[bool, str, float]:
        """
        Determine if a match is high confidence enough to assign license
        
        Returns:
            Tuple of (is_high_confidence, reason, confidence_score)
        """
        reasons = []
        confidence_factors = []
        
        # 1. EXACT UWI MATCH = Highest confidence
        bit_uwi = self.extract_uwi_component(bit_record.get('well_name', ''))
        gdc_uwi = self.extract_uwi_component(gdc_record.get('WELL_NAME', ''))
        
        if bit_uwi and gdc_uwi and bit_uwi == gdc_uwi:
            return True, "Exact UWI match", 1.0
        
        # 2. FULL LEGAL LOCATION MATCH + Additional validation
        bit_legal = self.extract_legal_location_components(bit_record.get('well_name', ''))
        gdc_legal = self.extract_legal_location_components(gdc_record.get('WELL_NAME', ''))
        
        if (bit_legal.get('full_legal') and gdc_legal.get('full_legal') and 
            bit_legal['full_legal'] == gdc_legal['full_legal']):
            
            confidence_factors.append(0.8)  # Strong legal location match
            reasons.append("Full legal location match")
            
            # Add coordinate validation if available
            if (pd.notna(bit_record.get('latitude')) and pd.notna(bit_record.get('longitude')) and
                pd.notna(gdc_record.get('LATITUDE')) and pd.notna(gdc_record.get('LONGITUDE'))):
                
                coord_diff = np.sqrt(
                    (float(bit_record['latitude']) - float(gdc_record['LATITUDE'])) ** 2 +
                    (float(bit_record['longitude']) - float(gdc_record['LONGITUDE'])) ** 2
                )
                
                if coord_diff < 0.001:  # Very close coordinates (~100m)
                    confidence_factors.append(0.15)
                    reasons.append("Matching coordinates")
                
            # Add spud date validation if available
            if (pd.notna(bit_record.get('spud_date')) and pd.notna(gdc_record.get('SPUD_DATE'))):
                spud_confidence, days_diff = self.calculate_spud_date_confidence(
                    bit_record['spud_date'], gdc_record['SPUD_DATE'], max_days_diff=30
                )
                
                if spud_confidence > 0.8:  # Within ~6 days
                    confidence_factors.append(0.1)
                    reasons.append(f"Spud date match (±{days_diff} days)")
            
            # Total confidence for legal location + extras
            total_confidence = sum(confidence_factors)
            
            # Require minimum confidence of 0.85 for legal location matches
            if total_confidence >= 0.85:
                return True, "; ".join(reasons), total_confidence
        
        # 3. EXACT WELL NAME MATCH + Strong coordinate/spud validation (for simple names)
        if match_type == 'exact_name' and bit_record.get('well_name') == gdc_record.get('WELL_NAME'):
            confidence_factors.append(0.6)  # Exact name match base
            reasons.append("Exact well name match")
            
            # Require strong additional validation for name-only matches
            coord_validated = False
            spud_validated = False
            
            # Coordinate validation (stricter for name matches)
            if (pd.notna(bit_record.get('latitude')) and pd.notna(bit_record.get('longitude')) and
                pd.notna(gdc_record.get('LATITUDE')) and pd.notna(gdc_record.get('LONGITUDE'))):
                
                coord_diff = np.sqrt(
                    (float(bit_record['latitude']) - float(gdc_record['LATITUDE'])) ** 2 +
                    (float(bit_record['longitude']) - float(gdc_record['LONGITUDE'])) ** 2
                )
                
                if coord_diff < 0.0005:  # Very strict coordinate match (~50m)
                    confidence_factors.append(0.25)
                    reasons.append("Precise coordinate match")
                    coord_validated = True
            
            # Spud date validation (stricter for name matches)
            if (pd.notna(bit_record.get('spud_date')) and pd.notna(gdc_record.get('SPUD_DATE'))):
                spud_confidence, days_diff = self.calculate_spud_date_confidence(
                    bit_record['spud_date'], gdc_record['SPUD_DATE'], max_days_diff=14
                )
                
                if spud_confidence > 0.9:  # Within ~1-2 days
                    confidence_factors.append(0.2)
                    reasons.append(f"Precise spud date match (±{days_diff} days)")
                    spud_validated = True
            
            # For exact name matches, require BOTH coordinate AND spud validation
            total_confidence = sum(confidence_factors)
            if coord_validated and spud_validated and total_confidence >= 0.9:
                return True, "; ".join(reasons), total_confidence
        
        # All other matches are considered too risky
        return False, "Insufficient confidence for safe assignment", sum(confidence_factors)
    
    def calculate_spud_date_confidence(self, bit_spud_date: Optional[str], gdc_spud_date: Optional[str], 
                                     max_days_diff: int = 30) -> Tuple[float, int]:
        """Calculate confidence score based on spud date proximity"""
        if pd.isna(bit_spud_date) or pd.isna(gdc_spud_date):
            return 0.0, -1
        
        try:
            bit_date = pd.to_datetime(bit_spud_date)
            gdc_date = pd.to_datetime(gdc_spud_date)
            
            days_diff = abs((bit_date - gdc_date).days)
            
            if days_diff <= max_days_diff:
                confidence = max(0.1, 1.0 - (days_diff / max_days_diff) * 0.9)
                return confidence, days_diff
            else:
                return 0.0, days_diff
                
        except (ValueError, TypeError):
            return 0.0, -1
    
    def safe_lookup_licenses(self, missing_df: pd.DataFrame, column_mapping: Dict[str, str]) -> Dict:
        """
        Perform safe license lookup with strict matching criteria
        Only returns high-confidence matches to prevent false assignments
        """
        results = {
            'high_confidence_matches': pd.DataFrame(),
            'rejected_matches': pd.DataFrame(),
            'unmatched_records': pd.DataFrame(),
            'summary': {}
        }
        
        # Validate required columns
        required_columns = ['license_number', 'well_name']
        for col in required_columns:
            if col not in column_mapping:
                logger.error(f"Required column '{col}' not found in GDC table mapping")
                return results
        
        license_col = column_mapping['license_number']
        well_name_col = column_mapping['well_name']
        latitude_col = column_mapping.get('surface_latitude')
        longitude_col = column_mapping.get('surface_longitude')
        spud_date_col = column_mapping.get('spud_date')
        
        logger.info(f"🔍 Starting SAFE license lookup for {len(missing_df)} records")
        logger.info("🛡️  Using strict matching criteria to prevent false assignments")
        
        high_confidence_matches = []
        rejected_matches = []
        unmatched_records = []
        
        # Process each record individually for careful matching
        for idx, record in missing_df.iterrows():
            well_name = record.get('well_name', '')
            if pd.isna(well_name) or not well_name:
                unmatched_records.append({
                    'original_index': idx,
                    'reason': 'No well name available',
                    **record.to_dict()
                })
                continue
            
            logger.debug(f"Processing well: {well_name}")
            
            # Build a comprehensive query to find potential matches
            potential_matches = []
            
            try:
                # Strategy 1: Look for exact well name matches first
                exact_query = f"""
                SELECT {license_col} as LICENSE_NUMBER,
                       {well_name_col} as WELL_NAME,
                       {latitude_col or 'NULL'} as LATITUDE,
                       {longitude_col or 'NULL'} as LONGITUDE,
                       {spud_date_col or 'NULL'} as SPUD_DATE
                FROM GDC.WELL 
                WHERE {well_name_col} = :well_name
                AND {license_col} IS NOT NULL
                """
                
                exact_matches = pd.read_sql(exact_query, self.connection, params={'well_name': well_name})
                
                # Apply trimming to license numbers from GDC for consistent matching
                if not exact_matches.empty and 'LICENSE_NUMBER' in exact_matches.columns:
                    exact_matches['LICENSE_NUMBER'] = exact_matches['LICENSE_NUMBER'].apply(self.trim_leading_zeros)
                
                for _, match in exact_matches.iterrows():
                    is_high_conf, reason, conf_score = self.is_high_confidence_match(record, match, 'exact_name')
                    potential_matches.append({
                        'match': match,
                        'is_high_confidence': is_high_conf,
                        'reason': reason,
                        'confidence_score': conf_score,
                        'match_type': 'exact_name'
                    })
                
                # Strategy 2: Look for legal location component matches (if no exact name match)
                if not any(m['is_high_confidence'] for m in potential_matches):
                    legal_components = self.extract_legal_location_components(well_name)
                    
                    if legal_components.get('full_legal'):
                        # Try to find wells with similar legal locations
                        legal_query = f"""
                        SELECT {license_col} as LICENSE_NUMBER,
                               {well_name_col} as WELL_NAME,
                               {latitude_col or 'NULL'} as LATITUDE,
                               {longitude_col or 'NULL'} as LONGITUDE,
                               {spud_date_col or 'NULL'} as SPUD_DATE
                        FROM GDC.WELL 
                        WHERE {well_name_col} LIKE :legal_pattern
                        AND {license_col} IS NOT NULL
                        """
                        
                        # Look for wells containing the legal location components
                        legal_pattern = f"%{legal_components['lsd']}-{legal_components['section']}-{legal_components['township']}-{legal_components['range']}{legal_components['meridian']}%"
                        
                        legal_matches = pd.read_sql(legal_query, self.connection, 
                                                  params={'legal_pattern': legal_pattern})
                        
                        # Apply trimming to license numbers from GDC for consistent matching
                        if not legal_matches.empty and 'LICENSE_NUMBER' in legal_matches.columns:
                            legal_matches['LICENSE_NUMBER'] = legal_matches['LICENSE_NUMBER'].apply(self.trim_leading_zeros)
                        
                        for _, match in legal_matches.iterrows():
                            is_high_conf, reason, conf_score = self.is_high_confidence_match(record, match, 'legal_location')
                            potential_matches.append({
                                'match': match,
                                'is_high_confidence': is_high_conf,
                                'reason': reason,
                                'confidence_score': conf_score,
                                'match_type': 'legal_location'
                            })
                
                # Select the best high-confidence match
                high_conf_matches = [m for m in potential_matches if m['is_high_confidence']]
                
                if high_conf_matches:
                    # Take the highest confidence match
                    best_match = max(high_conf_matches, key=lambda x: x['confidence_score'])
                    
                    high_confidence_matches.append({
                        'original_index': idx,
                        'well_name': well_name,
                        'found_license': best_match['match']['LICENSE_NUMBER'],
                        'gdc_well_name': best_match['match']['WELL_NAME'],
                        'match_type': best_match['match_type'],
                        'confidence_reason': best_match['reason'],
                        'confidence_score': best_match['confidence_score'],
                        **record.to_dict()
                    })
                    
                    logger.info(f"✅ High-confidence match found for {well_name}: {best_match['match']['LICENSE_NUMBER']}")
                
                else:
                    # Record why matches were rejected
                    if potential_matches:
                        best_rejected = max(potential_matches, key=lambda x: x['confidence_score'])
                        rejected_matches.append({
                            'original_index': idx,
                            'well_name': well_name,
                            'potential_license': best_rejected['match']['LICENSE_NUMBER'],
                            'gdc_well_name': best_rejected['match']['WELL_NAME'],
                            'rejection_reason': best_rejected['reason'],
                            'confidence_score': best_rejected['confidence_score'],
                            **record.to_dict()
                        })
                        
                        logger.debug(f"❌ Match rejected for {well_name}: {best_rejected['reason']}")
                    else:
                        unmatched_records.append({
                            'original_index': idx,
                            'reason': 'No potential matches found',
                            **record.to_dict()
                        })
                        
                        logger.debug(f"🔍 No matches found for {well_name}")
            
            except Exception as e:
                logger.error(f"❌ Error processing {well_name}: {e}")
                unmatched_records.append({
                    'original_index': idx,
                    'reason': f'Processing error: {str(e)}',
                    **record.to_dict()
                })
        
        # Compile results
        results['high_confidence_matches'] = pd.DataFrame(high_confidence_matches)
        results['rejected_matches'] = pd.DataFrame(rejected_matches)
        results['unmatched_records'] = pd.DataFrame(unmatched_records)
        
        # Summary statistics
        total_records = len(missing_df)
        high_conf_count = len(high_confidence_matches)
        rejected_count = len(rejected_matches)
        unmatched_count = len(unmatched_records)
        
        results['summary'] = {
            'total_missing_licenses': total_records,
            'high_confidence_matches': high_conf_count,
            'rejected_low_confidence': rejected_count,
            'unmatched_records': unmatched_count,
            'safe_assignment_rate': (high_conf_count / total_records * 100) if total_records > 0 else 0,
            'potential_matches_found': high_conf_count + rejected_count,
            'total_match_discovery_rate': ((high_conf_count + rejected_count) / total_records * 100) if total_records > 0 else 0
        }
        
        return results

def main():
    """Main function to perform safe GDC license lookup"""
    
    print("🛡️  SAFE GDC Oracle Database License Lookup")
    print("=" * 60)
    print("Using strict matching criteria to prevent false assignments")
    print("Only high-confidence, unambiguous matches will be recommended")
    
    lookup = SafeGDCLicenseLookup()
    
    try:
        # Connect to database
        if not lookup.connect():
            return
        
        # Explore table structure
        print("\n📊 Exploring GDC.WELL table structure...")
        structure_df, sample_df = lookup.explore_well_table_structure()
        
        if structure_df.empty:
            print("❌ Could not access GDC.WELL table")
            return
        
        # Find matching columns
        column_mapping = lookup.find_matching_columns(structure_df)
        
        if not column_mapping:
            print("❌ Could not identify required columns in GDC.WELL table")
            return
        
        # Load missing license records
        print(f"\n📥 Loading records missing license numbers...")
        missing_df = lookup.load_missing_license_records()
        
        if missing_df.empty:
            print("✅ No records missing license numbers!")
            return
        
        # Perform safe license lookup
        print(f"\n🔍 Starting SAFE license lookup process...")
        results = lookup.safe_lookup_licenses(missing_df, column_mapping)
        
        # Display results
        print(f"\n📊 SAFE LOOKUP RESULTS:")
        print("=" * 60)
        print(f"Total missing licenses: {results['summary']['total_missing_licenses']:,}")
        print(f"HIGH-CONFIDENCE matches: {results['summary']['high_confidence_matches']:,}")
        print(f"Rejected (low confidence): {results['summary']['rejected_low_confidence']:,}")
        print(f"No matches found: {results['summary']['unmatched_records']:,}")
        print(f"Safe assignment rate: {results['summary']['safe_assignment_rate']:.1f}%")
        print(f"Total discovery rate: {results['summary']['total_match_discovery_rate']:.1f}%")
        
        print(f"\n🛡️  SAFETY SUMMARY:")
        print("-" * 30)
        print(f"✅ SAFE to assign: {results['summary']['high_confidence_matches']} licenses")
        print(f"⚠️  REJECTED (risky): {results['summary']['rejected_low_confidence']} potential matches")
        print(f"❓ NO MATCHES: {results['summary']['unmatched_records']} records")
        
        # Save results
        output_dir = Path("Output")
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        
        if not results['high_confidence_matches'].empty or not results['rejected_matches'].empty:
            results_file = output_dir / f"SAFE_License_Lookup_Results_{timestamp}.xlsx"
            
            with pd.ExcelWriter(results_file) as writer:
                if not results['high_confidence_matches'].empty:
                    results['high_confidence_matches'].to_excel(writer, sheet_name='HIGH_CONFIDENCE_MATCHES', index=False)
                
                if not results['rejected_matches'].empty:
                    results['rejected_matches'].to_excel(writer, sheet_name='REJECTED_MATCHES', index=False)
                
                if not results['unmatched_records'].empty:
                    results['unmatched_records'].to_excel(writer, sheet_name='UNMATCHED', index=False)
            
            print(f"\n💾 Results saved to: {results_file.name}")
            
            if not results['high_confidence_matches'].empty:
                print(f"\n🎯 READY FOR APPLICATION:")
                print(f"   {len(results['high_confidence_matches'])} high-confidence license assignments")
                print(f"   These are safe to apply to your integrated dataset")
            
            if not results['rejected_matches'].empty:
                print(f"\n⚠️  MANUAL REVIEW NEEDED:")
                print(f"   {len(results['rejected_matches'])} potential matches were rejected")
                print(f"   Review these manually if needed (risk of false assignment)")
        else:
            print("\n❌ No license matches found")
        
    except Exception as e:
        logger.error(f"❌ Error in main process: {e}")
        
    finally:
        lookup.disconnect()

if __name__ == "__main__":
    main()
