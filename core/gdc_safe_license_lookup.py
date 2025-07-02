"""
Safe GDC License Lookup - Conservative Approach
Only makes high-confidence matches to avoid tying bit data to wrong wells.
Uses multiple verification criteria and compound keys (license + province).
"""

import pandas as pd
import numpy as np
from pathlib import Path
import oracledb
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SafeGDCLicenseLookup:
    """Conservative GDC Oracle database lookup to avoid false matches"""
    
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
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Disconnected from Oracle database")
    
    def infer_province_from_longitude(self, longitude: float) -> str:
        """
        Infer province from longitude using Alberta-BC border at 120°W
        
        Args:
            longitude: Longitude in decimal degrees (negative for west)
            
        Returns:
            'AB' for Alberta, 'BC' for British Columbia, 'UNKNOWN' if unclear
        """
        if pd.isna(longitude):
            return 'UNKNOWN'
        
        # Alberta-BC border is at 120°W (-120.0 in decimal degrees)
        if longitude > -120.0:
            return 'AB'  # East of border = Alberta
        elif longitude < -120.0:
            return 'BC'  # West of border = British Columbia
        else:
            return 'UNKNOWN'  # Exactly on border (rare)
    
    def extract_legal_location_from_name(self, well_name: str) -> Optional[str]:
        """
        Extract legal location pattern from well name
        Alberta format: LSD-SEC-TWP-RGE W4M/W5M/W6M
        
        Returns the legal location string if found, None otherwise
        """
        if pd.isna(well_name) or not isinstance(well_name, str):
            return None
        
        # Pattern for Alberta legal locations: number-number-number-numberW4M/W5M/W6M
        # Examples: 1-2-3-4W5, 100/12-34-56-78W4, etc.
        pattern = r'(\d+/?)*(\d+-\d+-\d+-\d+W[456]M?)'
        
        match = re.search(pattern, well_name.upper())
        if match:
            return match.group(2)  # Return the legal location part
        
        return None
    
    def calculate_coordinate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate approximate distance between two coordinates in kilometers
        Using simple Euclidean distance for small distances
        """
        if any(pd.isna([lat1, lon1, lat2, lon2])):
            return float('inf')
        
        # Convert to approximate km (rough approximation for small distances)
        lat_diff_km = (lat2 - lat1) * 111.0  # ~111 km per degree latitude
        lon_diff_km = (lon2 - lon1) * 111.0 * np.cos(np.radians((lat1 + lat2) / 2))  # Adjust for longitude
        
        distance = np.sqrt(lat_diff_km**2 + lon_diff_km**2)
        return distance
    
    def safe_coordinate_lookup(self, missing_df: pd.DataFrame) -> pd.DataFrame:
        """
        Conservative coordinate-based lookup with multiple verification criteria
        """
        logger.info("🎯 Starting SAFE coordinate-based lookup...")
        
        # Add province inference to the dataframe
        missing_df = missing_df.copy()
        missing_df['inferred_province'] = missing_df['longitude'].apply(self.infer_province_from_longitude)
        
        logger.info(f"📍 Province distribution:")
        province_counts = missing_df['inferred_province'].value_counts()
        for province, count in province_counts.items():
            logger.info(f"   {province}: {count} wells")
        
        safe_matches = []
        tolerance = 0.001  # ~100m tolerance for coordinates
        
        try:
            # Process each record individually for maximum safety
            for idx, row in missing_df.iterrows():
                if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                    continue
                
                province = row['inferred_province']
                if province == 'UNKNOWN':
                    continue
                
                # Query GDC for wells in same province with similar coordinates
                query = """
                SELECT WELL_NUM, WELL_NAME, OPERATOR, PROVINCE_STATE,
                       SURFACE_LATITUDE, SURFACE_LONGITUDE, SPUD_DATE,
                       ASSIGNED_FIELD
                FROM GDC.WELL 
                WHERE PROVINCE_STATE = :province
                AND WELL_NUM IS NOT NULL
                AND SURFACE_LATITUDE BETWEEN :lat_min AND :lat_max
                AND SURFACE_LONGITUDE BETWEEN :lon_min AND :lon_max
                """
                
                params = {
                    'province': province,
                    'lat_min': row['latitude'] - tolerance,
                    'lat_max': row['latitude'] + tolerance,
                    'lon_min': row['longitude'] - tolerance,
                    'lon_max': row['longitude'] + tolerance
                }
                
                candidates = pd.read_sql(query, self.connection, params=params)
                
                if candidates.empty:
                    continue
                
                # Find the best match within tolerance
                for _, gdc_row in candidates.iterrows():
                    distance = self.calculate_coordinate_distance(
                        row['latitude'], row['longitude'],
                        gdc_row['SURFACE_LATITUDE'], gdc_row['SURFACE_LONGITUDE']
                    )
                    
                    # Very strict distance requirement (< 100m)
                    if distance > 0.1:  # 100 meters
                        continue
                    
                    # Additional verification criteria
                    verification_score = 0
                    verification_details = []
                    
                    # 1. Spud date verification (if available)
                    if not pd.isna(row['spud_date']) and not pd.isna(gdc_row['SPUD_DATE']):
                        try:
                            bit_spud = pd.to_datetime(row['spud_date'])
                            gdc_spud = pd.to_datetime(gdc_row['SPUD_DATE'])
                            days_diff = abs((bit_spud - gdc_spud).days)
                            
                            if days_diff <= 7:  # Within 1 week
                                verification_score += 3
                                verification_details.append(f"spud_match_{days_diff}d")
                            elif days_diff <= 30:  # Within 1 month
                                verification_score += 1
                                verification_details.append(f"spud_close_{days_diff}d")
                        except:
                            pass
                    
                    # 2. Field name verification (if available)
                    if not pd.isna(row['field']) and not pd.isna(gdc_row['ASSIGNED_FIELD']):
                        if str(row['field']).upper() in str(gdc_row['ASSIGNED_FIELD']).upper():
                            verification_score += 2
                            verification_details.append("field_match")
                    
                    # 3. Operator verification (if available)
                    if not pd.isna(row['operator']) and not pd.isna(gdc_row['OPERATOR']):
                        if str(row['operator']).upper() in str(gdc_row['OPERATOR']).upper() or \
                           str(gdc_row['OPERATOR']).upper() in str(row['operator']).upper():
                            verification_score += 2
                            verification_details.append("operator_match")
                    
                    # 4. Legal location in well name (if extractable)
                    legal_location = self.extract_legal_location_from_name(row['well_name'])
                    if legal_location and legal_location in str(gdc_row['WELL_NAME']).upper():
                        verification_score += 3
                        verification_details.append("legal_location_match")
                    
                    # Only accept matches with high verification score
                    if verification_score >= 3:  # Require at least 3 points of verification
                        confidence = "high" if verification_score >= 5 else "medium"
                        
                        safe_matches.append({
                            'original_index': idx,
                            'well_name': row['well_name'],
                            'gdc_well_name': gdc_row['WELL_NAME'],
                            'operator': row['operator'],
                            'field': row['field'],
                            'found_license': int(gdc_row['WELL_NUM']),
                            'province': province,
                            'coordinate_distance_m': round(distance * 1000, 1),
                            'verification_score': verification_score,
                            'verification_details': ';'.join(verification_details),
                            'match_method': f'safe_coordinate_{province}',
                            'confidence': confidence
                        })
                        
                        logger.info(f"✅ Safe match found: {row['well_name']} → License {int(gdc_row['WELL_NUM'])} " +
                                  f"({province}, {distance*1000:.1f}m, score={verification_score})")
                        break  # Take first high-confidence match only
        
        except Exception as e:
            logger.error(f"❌ Error in safe coordinate lookup: {e}")
            return pd.DataFrame()
        
        if safe_matches:
            return pd.DataFrame(safe_matches)
        else:
            logger.info("❌ No safe coordinate matches found")
            return pd.DataFrame()
    
    def safe_well_name_lookup(self, missing_df: pd.DataFrame) -> pd.DataFrame:
        """
        Conservative well name lookup - only exact matches with multiple verification
        """
        logger.info("🎯 Starting SAFE well name lookup...")
        
        # Add province inference
        missing_df = missing_df.copy()
        missing_df['inferred_province'] = missing_df['longitude'].apply(self.infer_province_from_longitude)
        
        safe_matches = []
        
        try:
            # Get unique well names by province
            for province in ['AB', 'BC']:
                province_wells = missing_df[missing_df['inferred_province'] == province]
                if province_wells.empty:
                    continue
                
                unique_wells = province_wells['well_name'].dropna().unique()
                logger.info(f"🔍 Checking {len(unique_wells)} unique well names in {province}")
                
                if len(unique_wells) == 0:
                    continue
                
                # Query for exact well name matches in the same province
                placeholders = ','.join([f':well{i}' for i in range(len(unique_wells))])
                query = f"""
                SELECT WELL_NUM, WELL_NAME, OPERATOR, PROVINCE_STATE,
                       SURFACE_LATITUDE, SURFACE_LONGITUDE, SPUD_DATE,
                       ASSIGNED_FIELD
                FROM GDC.WELL 
                WHERE PROVINCE_STATE = :province
                AND WELL_NUM IS NOT NULL
                AND UPPER(WELL_NAME) IN ({placeholders})
                """
                
                params = {'province': province}
                for i, well_name in enumerate(unique_wells):
                    params[f'well{i}'] = well_name.upper()
                
                exact_matches = pd.read_sql(query, self.connection, params=params)
                logger.info(f"📊 Found {len(exact_matches)} exact name matches in {province}")
                
                # Verify each exact match with additional criteria
                for _, exact_row in exact_matches.iterrows():
                    gdc_well_name = exact_row['WELL_NAME'].upper()
                    
                    # Find corresponding bit data records
                    matching_bit_records = province_wells[
                        province_wells['well_name'].str.upper() == gdc_well_name
                    ]
                    
                    for bit_idx, bit_row in matching_bit_records.iterrows():
                        verification_score = 3  # Start with 3 for exact name match
                        verification_details = ['exact_name_match']
                        
                        # Additional verification
                        # 1. Coordinate proximity (if available)
                        if not pd.isna(bit_row['latitude']) and not pd.isna(exact_row['SURFACE_LATITUDE']):
                            distance = self.calculate_coordinate_distance(
                                bit_row['latitude'], bit_row['longitude'],
                                exact_row['SURFACE_LATITUDE'], exact_row['SURFACE_LONGITUDE']
                            )
                            if distance <= 1.0:  # Within 1km
                                verification_score += 2
                                verification_details.append(f"coord_close_{distance:.1f}km")
                        
                        # 2. Spud date verification
                        if not pd.isna(bit_row['spud_date']) and not pd.isna(exact_row['SPUD_DATE']):
                            try:
                                bit_spud = pd.to_datetime(bit_row['spud_date'])
                                gdc_spud = pd.to_datetime(exact_row['SPUD_DATE'])
                                days_diff = abs((bit_spud - gdc_spud).days)
                                
                                if days_diff <= 7:
                                    verification_score += 3
                                    verification_details.append(f"spud_match_{days_diff}d")
                                elif days_diff <= 30:
                                    verification_score += 1
                                    verification_details.append(f"spud_close_{days_diff}d")
                            except:
                                pass
                        
                        # 3. Operator verification
                        if not pd.isna(bit_row['operator']) and not pd.isna(exact_row['OPERATOR']):
                            if str(bit_row['operator']).upper() in str(exact_row['OPERATOR']).upper() or \
                               str(exact_row['OPERATOR']).upper() in str(bit_row['operator']).upper():
                                verification_score += 2
                                verification_details.append("operator_match")
                        
                        # Only accept high-confidence matches
                        if verification_score >= 5:  # Require high verification for name matches
                            confidence = "high" if verification_score >= 7 else "medium"
                            
                            safe_matches.append({
                                'original_index': bit_idx,
                                'well_name': bit_row['well_name'],
                                'gdc_well_name': exact_row['WELL_NAME'],
                                'operator': bit_row['operator'],
                                'field': bit_row['field'],
                                'found_license': int(exact_row['WELL_NUM']),
                                'province': province,
                                'verification_score': verification_score,
                                'verification_details': ';'.join(verification_details),
                                'match_method': f'safe_exact_name_{province}',
                                'confidence': confidence
                            })
                            
                            logger.info(f"✅ Safe name match: {bit_row['well_name']} → License {int(exact_row['WELL_NUM'])} " +
                                      f"({province}, score={verification_score})")
        
        except Exception as e:
            logger.error(f"❌ Error in safe well name lookup: {e}")
            return pd.DataFrame()
        
        if safe_matches:
            return pd.DataFrame(safe_matches)
        else:
            logger.info("❌ No safe well name matches found")
            return pd.DataFrame()

def main():
    """Main execution function"""
    print("🛡️  SAFE GDC License Lookup - Conservative Approach")
    print("=" * 60)
    
    lookup = SafeGDCLicenseLookup()
    
    # Connect to database
    if not lookup.connect():
        return
    
    try:
        # Load missing license data
        output_dir = Path("Output")
        integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
        
        if not integrated_files:
            logger.error("❌ No integrated dataset found!")
            return
        
        latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"📊 Loading data from: {latest_file.name}")
        
        df = pd.read_excel(latest_file)
        missing_df = df[df['license_number'].isna()].copy()
        
        logger.info(f"🔍 Found {len(missing_df)} records missing license numbers")
        
        if missing_df.empty:
            logger.info("✅ No missing licenses to lookup!")
            return
        
        # Perform safe lookups
        all_matches = []
        
        # 1. Safe coordinate lookup
        coord_matches = lookup.safe_coordinate_lookup(missing_df)
        if not coord_matches.empty:
            all_matches.append(coord_matches)
            logger.info(f"✅ Safe coordinate matches: {len(coord_matches)}")
        
        # 2. Safe well name lookup
        name_matches = lookup.safe_well_name_lookup(missing_df)
        if not name_matches.empty:
            all_matches.append(name_matches)
            logger.info(f"✅ Safe well name matches: {len(name_matches)}")
        
        # Combine and save results
        if all_matches:
            final_results = pd.concat(all_matches, ignore_index=True)
            
            # Remove duplicates (same original_index)
            final_results = final_results.drop_duplicates(subset=['original_index'], keep='first')
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"Safe_License_Lookup_Results_{timestamp}.xlsx"
            output_path = output_dir / output_filename
            
            final_results.to_excel(output_path, index=False)
            
            logger.info(f"💾 Results saved to: {output_filename}")
            
            # Summary
            print(f"\n📊 SAFE LOOKUP RESULTS:")
            print("=" * 30)
            print(f"  Total missing licenses: {len(missing_df)}")
            print(f"  Safe matches found: {len(final_results)}")
            print(f"  Success rate: {len(final_results)/len(missing_df)*100:.1f}%")
            
            confidence_counts = final_results['confidence'].value_counts()
            for conf, count in confidence_counts.items():
                print(f"  {str(conf).title()} confidence: {count}")
            
        else:
            logger.info("❌ No safe matches found")
            print(f"\n📊 SAFE LOOKUP RESULTS:")
            print("=" * 30)
            print(f"  Total missing licenses: {len(missing_df)}")
            print(f"  Safe matches found: 0")
            print(f"  Success rate: 0.0%")
            print(f"\n💡 The conservative approach found no matches that meet")
            print(f"     the strict verification criteria to avoid false positives.")
    
    finally:
        lookup.disconnect()

if __name__ == "__main__":
    main()
