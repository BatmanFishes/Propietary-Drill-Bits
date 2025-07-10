"""
Oracle GDC Well Table License Lookup
Connect to the GDC Oracle database to find missing license numbers for wells
"""

import pandas as pd
import numpy as np
from pathlib import Path
import oracledb
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GDCLicenseLookup:
    """Connect to GDC Oracle database to lookup missing license numbers"""
    
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
            # Build connection string
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
    
    def explore_well_table_structure(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Explore the structure of the GDC.WELL table"""
        if not self.connection:
            logger.error("No database connection")
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            # Get table structure
            structure_query = """
            SELECT column_name, data_type, data_length, nullable
            FROM all_tab_columns 
            WHERE owner = 'GDC' AND table_name = 'WELL'
            ORDER BY column_id
            """
            
            structure_df = pd.read_sql(structure_query, self.connection)
            
            logger.info(f"📊 GDC.WELL table has {len(structure_df)} columns")
            
            # Get sample data
            sample_query = "SELECT * FROM GDC.WELL WHERE ROWNUM <= 5"
            sample_df = pd.read_sql(sample_query, self.connection)
            
            logger.info(f"📋 Sample data retrieved: {len(sample_df)} rows")
            
            return structure_df, sample_df
            
        except Exception as e:
            logger.error(f"❌ Error exploring table structure: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def find_matching_columns(self, structure_df: pd.DataFrame) -> Dict[str, str]:
        """Identify relevant columns for well matching"""
        
        # Common column name patterns for well identification
        column_patterns = {
            'license_number': ['WELL_NUM', 'WELL_GOVERNMENT_ID', 'LICENSE', 'LIC', 'PERMIT', 'WELL_LIC'],
            'well_name': ['WELL_NAME', 'NAME', 'WELLNAME', 'WELL_ID'],
            'operator': ['OPERATOR', 'COMPANY', 'LICENSEE'],
            'uwi': ['UWI', 'GSL_UWI', 'API', 'WELL_UWI'],
            'field': ['ASSIGNED_FIELD', 'FIELD', 'FIELD_NAME'],
            'surface_latitude': ['SURFACE_LATITUDE', 'LAT', 'LATITUDE', 'Y_COORD'],
            'surface_longitude': ['SURFACE_LONGITUDE', 'LON', 'LONG', 'LONGITUDE', 'X_COORD'],
            'bottom_latitude': ['BOTTOM_HOLE_LATITUDE'],
            'bottom_longitude': ['BOTTOM_HOLE_LONGITUDE'],
            'spud_date': ['SPUD_DATE', 'SPUD', 'DRILL_DATE'],
            'surface_location': ['SURFACE', 'LSD', 'LOCATION']
        }
        
        column_mapping = {}
        available_columns = structure_df['COLUMN_NAME'].str.upper().tolist()
        
        print(f"\n🔍 AVAILABLE GDC.WELL COLUMNS:")
        print("=" * 50)
        for col in available_columns:
            print(f"  {col}")
        
        print(f"\n🎯 SUGGESTED COLUMN MAPPINGS:")
        print("=" * 50)
        
        for purpose, patterns in column_patterns.items():
            found_column = None
            for pattern in patterns:
                matching_cols = [col for col in available_columns if pattern in col]
                if matching_cols:
                    found_column = matching_cols[0]  # Take first match
                    break
            
            if found_column:
                column_mapping[purpose] = found_column
                print(f"  {purpose:15s} -> {found_column}")
            else:
                print(f"  {purpose:15s} -> NOT FOUND")
        
        return column_mapping
    
    def calculate_spud_date_confidence(self, bit_spud_date: Optional[str], gdc_spud_date: Optional[str], 
                                     max_days_diff: int = 60) -> Tuple[float, int]:
        """
        Calculate confidence score based on spud date proximity
        
        Args:
            bit_spud_date: Spud date from bit data (string)
            gdc_spud_date: Spud date from GDC (string or datetime)
            max_days_diff: Maximum days difference for a match (default 60)
            
        Returns:
            Tuple of (confidence_score, days_difference)
            confidence_score: 0.0 to 1.0, where 1.0 is exact match
            days_difference: absolute days between dates (or -1 if invalid)
        """
        if pd.isna(bit_spud_date) or pd.isna(gdc_spud_date):
            return 0.0, -1
        
        try:
            # Convert to datetime if needed
            if isinstance(bit_spud_date, str):
                bit_date = pd.to_datetime(bit_spud_date)
            else:
                bit_date = pd.to_datetime(bit_spud_date)
                
            if isinstance(gdc_spud_date, str):
                gdc_date = pd.to_datetime(gdc_spud_date)
            else:
                gdc_date = pd.to_datetime(gdc_spud_date)
            
            # Calculate days difference
            days_diff = abs((bit_date - gdc_date).days)
            
            if days_diff <= max_days_diff:
                # Linear confidence decrease from 1.0 (exact match) to 0.1 (at max_days_diff)
                confidence = max(0.1, 1.0 - (days_diff / max_days_diff) * 0.9)
                return confidence, days_diff
            else:
                return 0.0, days_diff
                
        except (ValueError, TypeError) as e:
            logger.debug(f"Error parsing dates: {bit_spud_date}, {gdc_spud_date} - {e}")
            return 0.0, -1
    
    def load_missing_license_records(self) -> pd.DataFrame:
        """Load records that are missing license numbers"""
        
        # Find the most recent integrated file
        output_dir = Path("Output")
        integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
        if not integrated_files:
            logger.error("No integrated data files found")
            return pd.DataFrame()
        
        latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"📊 Loading missing license data from: {latest_file.name}")
        
        # Load integrated data
        df = pd.read_excel(latest_file)
        
        # Filter to Ulterra records missing license numbers
        missing_license = df[
            (df['data_source'] == 'ulterra') &
            ((df['license_number'].isna()) | (df['license_number'] == ''))
        ].copy()
        
        logger.info(f"🔍 Found {len(missing_license)} records missing license numbers")
        
        return missing_license
    
    def normalize_well_name(self, well_name: str) -> str:
        """
        Normalize well name by removing common operator prefixes and standardizing format.
        Handles special characters including slashes, which are common in well names.
        """
        if pd.isna(well_name) or not isinstance(well_name, str):
            return ""
        
        # Convert to uppercase and strip whitespace
        normalized = well_name.upper().strip()
        
        # Common operator prefixes that might change due to well sales/exchanges
        operator_prefixes = [
            'TOURMALINE', 'TOU', 'WHITECAP', 'WCP', 'PARAMOUNT', 'POU', 'CNRL', 'COP',
            'ENCANA', 'OVV', 'CHEVRON', 'CVX', 'SHELL', 'IMPERIAL', 'IMO', 'SUNCOR',
            'HUSKY', 'EXXON', 'BP', 'TOTAL', 'EQUINOR', 'CONOCOPHILLIPS', 'KELT',
            'PEYTO', 'BIRCHCLIFF', 'ARC', 'CANADIAN NATURAL', 'ATHABASCA OIL',
            'VERMILION', 'VET', 'BAYTEX', 'BTE', 'CRESCENT POINT', 'CPG', 'GIBSON'
        ]
        
        # Try to remove operator prefix if present
        for prefix in operator_prefixes:
            # Remove prefix followed by space, dash, underscore, or slash
            patterns = [f"{prefix} ", f"{prefix}-", f"{prefix}_", f"{prefix}/"]
            for pattern in patterns:
                if normalized.startswith(pattern):
                    normalized = normalized[len(pattern):].strip()
                    break
        
        # Handle special characters and separators
        # Replace slashes and other separators with spaces for consistent matching
        # This addresses the 84.6% of unmatched wells that have slashes
        normalized = normalized.replace('/', ' ')  # Most common issue from audit
        normalized = normalized.replace('\\', ' ')  # Backslashes
        normalized = normalized.replace('-', ' ')   # Dashes
        normalized = normalized.replace('_', ' ')   # Underscores
        normalized = normalized.replace('.', ' ')   # Periods
        normalized = normalized.replace(',', ' ')   # Commas
        normalized = normalized.replace('(', ' ')   # Left parenthesis
        normalized = normalized.replace(')', ' ')   # Right parenthesis
        normalized = normalized.replace('[', ' ')   # Left bracket
        normalized = normalized.replace(']', ' ')   # Right bracket
        normalized = normalized.replace('{', ' ')   # Left brace
        normalized = normalized.replace('}', ' ')   # Right brace
        normalized = normalized.replace('|', ' ')   # Pipe character
        normalized = normalized.replace('+', ' ')   # Plus sign
        normalized = normalized.replace('=', ' ')   # Equals sign
        normalized = normalized.replace('&', ' ')   # Ampersand
        normalized = normalized.replace('#', ' ')   # Hash
        normalized = normalized.replace('@', ' ')   # At symbol
        normalized = normalized.replace('*', ' ')   # Asterisk
        normalized = normalized.replace('%', ' ')   # Percent
        normalized = normalized.replace('$', ' ')   # Dollar sign
        normalized = normalized.replace('^', ' ')   # Caret
        normalized = normalized.replace('!', ' ')   # Exclamation
        normalized = normalized.replace('?', ' ')   # Question mark
        normalized = normalized.replace(':', ' ')   # Colon
        normalized = normalized.replace(';', ' ')   # Semicolon
        normalized = normalized.replace('"', ' ')   # Double quote
        normalized = normalized.replace("'", ' ')   # Single quote
        normalized = normalized.replace('`', ' ')   # Backtick
        normalized = normalized.replace('~', ' ')   # Tilde
        
        # Remove multiple spaces and normalize whitespace
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    def calculate_well_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two well names using multiple methods"""
        try:
            from difflib import SequenceMatcher
        except ImportError:
            # Fallback to simple comparison if difflib not available
            return 1.0 if name1.upper() == name2.upper() else 0.0
        
        if not name1 or not name2:
            return 0.0
        
        # Normalize both names
        norm1 = self.normalize_well_name(name1)
        norm2 = self.normalize_well_name(name2)
        
        if not norm1 or not norm2:
            return 0.0
        
        # Exact match after normalization
        if norm1 == norm2:
            return 1.0
        
        # Calculate sequence similarity
        similarity = SequenceMatcher(None, norm1, norm2).ratio()
        
        # Bonus for containing the same core well identifier
        # Look for common patterns like "1-2-3-4W5" or "100/1-2-3-4W5"
        import re
        
        # Extract potential well identifiers (numbers followed by well location)
        pattern = r'(\d+/?\d*-\d+-\d+-\d+W\d+|\d+-\d+-\d+-\d+W\d+)'
        match1 = re.search(pattern, norm1)
        match2 = re.search(pattern, norm2)
        
        if match1 and match2 and match1.group(1) == match2.group(1):
            similarity = max(similarity, 0.85)  # High confidence for same legal location
        
        return similarity
    
    def lookup_licenses_by_well_name(self, missing_df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """Enhanced lookup using well name matching with field-based filtering via WELL_NAME patterns"""
        
        if 'license_number' not in column_mapping or 'well_name' not in column_mapping:
            logger.error("Required columns (license_number, well_name) not found in GDC table")
            return pd.DataFrame()
        
        # Get unique well names and fields that need license lookup
        wells_to_lookup = missing_df[missing_df['well_name'].notna()]['well_name'].unique()
        fields_to_lookup = missing_df[missing_df['field'].notna()]['field'].unique()
        
        logger.info(f"🔍 Looking up {len(wells_to_lookup)} unique well names across {len(fields_to_lookup)} fields")
        logger.info(f"📍 Target fields: {', '.join(fields_to_lookup[:5])}{'...' if len(fields_to_lookup) > 5 else ''}")
        
        license_col = column_mapping['license_number']
        well_name_col = column_mapping['well_name']
        
        all_matches = []
        
        try:
            # Build field-based filter using WELL_NAME patterns (since field names are embedded in well names)
            field_conditions = []
            field_params = {}
            
            for idx, field in enumerate(fields_to_lookup):
                # Create LIKE patterns to find wells with field names in their well names
                field_conditions.append(f"UPPER({well_name_col}) LIKE :field_pattern{idx}")
                field_params[f'field_pattern{idx}'] = f"%{field.upper()}%"
            
            field_filter = ""
            if field_conditions:
                field_filter = f"AND ({' OR '.join(field_conditions)})"
                logger.info(f"📍 Filtering GDC wells by field patterns in well names")
            
            # First, try exact well name matches with field filtering
            logger.info("📍 Phase 1: Exact well name matches (field-pattern-filtered)...")
            well_placeholders = ','.join([':well' + str(i) for i in range(len(wells_to_lookup))])
            
            exact_query = f"""
            SELECT DISTINCT {license_col} as LICENSE_NUMBER, 
                           {well_name_col} as WELL_NAME
            FROM GDC.WELL 
            WHERE {well_name_col} IN ({well_placeholders})
            AND {license_col} IS NOT NULL
            {field_filter}
            """
            
            # Combine well and field parameters
            params = {f'well{i}': well_name for i, well_name in enumerate(wells_to_lookup)}
            params.update(field_params)
            
            exact_matches = pd.read_sql(exact_query, self.connection, params=params)
            
            if not exact_matches.empty:
                exact_matches['match_type'] = 'exact'
                exact_matches['similarity'] = 1.0
                all_matches.append(exact_matches)
                logger.info(f"✅ Found {len(exact_matches)} exact well name matches (field-pattern-filtered)")
            
            # Phase 1.5: Try normalized exact matches for wells that didn't match exactly
            # This addresses the 84.6% of wells with special characters like slashes
            exact_match_names = set(exact_matches['WELL_NAME']) if not exact_matches.empty else set()
            wells_for_normalized = [w for w in wells_to_lookup if w not in exact_match_names]
            
            if wells_for_normalized:
                logger.info(f"📍 Phase 1.5: Normalized exact matches for {len(wells_for_normalized)} wells (handling special characters)...")
                
                # Get all candidate wells from the field-filtered set for normalization comparison
                candidates_query = f"""
                SELECT DISTINCT {license_col} as LICENSE_NUMBER, 
                               {well_name_col} as WELL_NAME
                FROM GDC.WELL 
                WHERE {license_col} IS NOT NULL 
                AND {well_name_col} IS NOT NULL
                {field_filter}
                """
                
                candidate_wells = pd.read_sql(candidates_query, self.connection, params=field_params)
                
                normalized_matches = []
                for bit_well in wells_for_normalized:
                    normalized_bit_well = self.normalize_well_name(bit_well)
                    
                    for _, gdc_row in candidate_wells.iterrows():
                        normalized_gdc_well = self.normalize_well_name(gdc_row['WELL_NAME'])
                        
                        if normalized_bit_well == normalized_gdc_well and normalized_bit_well:  # Exact match after normalization
                            normalized_matches.append({
                                'LICENSE_NUMBER': gdc_row['LICENSE_NUMBER'],
                                'WELL_NAME': gdc_row['WELL_NAME'],
                                'BIT_WELL_NAME': bit_well,
                                'match_type': 'normalized_exact',
                                'similarity': 1.0
                            })
                            break  # Take first match for each bit well
                
                if normalized_matches:
                    normalized_df = pd.DataFrame(normalized_matches)
                    all_matches.append(normalized_df)
                    logger.info(f"✅ Found {len(normalized_matches)} normalized exact matches (special characters handled)")
            
            # Get wells that didn't have exact or normalized matches for fuzzy matching
            all_matched_names = set()
            if not exact_matches.empty:
                all_matched_names.update(exact_matches['WELL_NAME'])
            
            # Check if we have normalized matches
            normalized_df = None
            for match_df in all_matches:
                if 'match_type' in match_df.columns and 'normalized_exact' in match_df['match_type'].values:
                    normalized_df = match_df
                    break
            
            if normalized_df is not None and not normalized_df.empty:
                all_matched_names.update(normalized_df['BIT_WELL_NAME'])
            
            wells_for_fuzzy = [w for w in wells_to_lookup if w not in all_matched_names]
            
            if wells_for_fuzzy and len(wells_for_fuzzy) <= 30:  # Reasonable limit for fuzzy matching
                logger.info(f"🔄 Phase 2: Fuzzy matching for {len(wells_for_fuzzy)} remaining wells (field-pattern-filtered)...")
                
                # Get field-pattern-filtered well names from GDC for fuzzy matching
                fuzzy_query = f"""
                SELECT DISTINCT {license_col} as LICENSE_NUMBER, 
                               {well_name_col} as WELL_NAME
                FROM GDC.WELL 
                WHERE {license_col} IS NOT NULL 
                AND {well_name_col} IS NOT NULL
                {field_filter}
                AND ROWNUM <= 10000
                """
                
                candidate_wells = pd.read_sql(fuzzy_query, self.connection, params=field_params)
                logger.info(f"📊 Found {len(candidate_wells)} candidate wells for fuzzy matching (field-pattern-filtered)")
                
                # Perform fuzzy matching on the field-filtered candidate set
                fuzzy_matches = []
                for bit_well in wells_for_fuzzy:
                    best_match = None
                    best_similarity = 0.0
                    
                    for _, gdc_row in candidate_wells.iterrows():
                        gdc_well = gdc_row['WELL_NAME']
                        similarity = self.calculate_well_name_similarity(bit_well, gdc_well)
                        
                        if similarity > best_similarity and similarity >= 0.75:
                            best_similarity = similarity
                            best_match = {
                                'LICENSE_NUMBER': gdc_row['LICENSE_NUMBER'],
                                'WELL_NAME': gdc_well,
                                'BIT_WELL_NAME': bit_well,
                                'match_type': 'fuzzy',
                                'similarity': similarity
                            }
                    
                    if best_match:
                        fuzzy_matches.append(best_match)
                
                if fuzzy_matches:
                    fuzzy_df = pd.DataFrame(fuzzy_matches)
                    # Only keep high-confidence fuzzy matches (>85% similarity)
                    high_conf_fuzzy = fuzzy_df[fuzzy_df['similarity'] >= 0.85]
                    
                    if not high_conf_fuzzy.empty:
                        all_matches.append(high_conf_fuzzy)
                        logger.info(f"✅ Found {len(high_conf_fuzzy)} high-confidence fuzzy matches (field-pattern-filtered)")
            elif len(wells_for_fuzzy) > 30:
                logger.info(f"⏭️  Skipping fuzzy matching for {len(wells_for_fuzzy)} wells (too many for efficient processing)")
            
            # Combine all matches
            if all_matches:
                final_results = pd.concat(all_matches, ignore_index=True)
                logger.info(f"✅ Total well name matches: {len(final_results)}")
                return final_results
            else:
                logger.info("❌ No well name matches found")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error in field-pattern-filtered well name lookup: {e}")
            return pd.DataFrame()
    
    def lookup_licenses_by_coordinates(self, missing_df: pd.DataFrame, column_mapping: Dict[str, str], tolerance: float = 0.001) -> pd.DataFrame:
        """Lookup license numbers using coordinate matching with field filtering, tolerance and optional spud date matching"""
        
        # Check which coordinate columns are available - ONLY use surface coordinates
        latitude_col = None
        longitude_col = None
        spud_date_col = None
        field_col = None
        
        if 'surface_latitude' in column_mapping and 'surface_longitude' in column_mapping:
            latitude_col = column_mapping['surface_latitude']
            longitude_col = column_mapping['surface_longitude']
        else:
            logger.error("Surface coordinates not found in GDC table mapping - cannot use bottom hole coordinates for well identification")
            return pd.DataFrame()
        
        if 'spud_date' in column_mapping:
            spud_date_col = column_mapping['spud_date']
            logger.info(f"📅 Spud date column found: {spud_date_col} - will use for enhanced matching")
        else:
            logger.warning("⚠️  Spud date column not found - coordinate matching only")
        
        if 'field' in column_mapping:
            field_col = column_mapping['field']
            logger.info(f"📍 Field column found: {field_col} - will use for filtering")
        
        if 'license_number' not in column_mapping:
            logger.error("License number column not found in GDC table mapping")
            return pd.DataFrame()
        
        # Get records with coordinates and fields for field-based filtering
        coord_records = missing_df[
            missing_df['latitude'].notna() & 
            missing_df['longitude'].notna()
        ].copy()
        
        if len(coord_records) == 0:
            logger.info("No records with coordinates for lookup")
            return pd.DataFrame()
        
        # Get unique fields for filtering
        fields_to_lookup = coord_records[coord_records['field'].notna()]['field'].unique()
        
        logger.info(f"🗺️  Looking up {len(coord_records)} records by coordinates" + 
                   (" and spud dates" if spud_date_col else "") +
                   (f" (field-filtered: {len(fields_to_lookup)} fields)" if field_col and len(fields_to_lookup) > 0 else ""))
        
        license_col = column_mapping['license_number']
        well_name_col = column_mapping.get('well_name', 'NULL')
        
        # Build field filter condition
        field_filter = ""
        field_params = {}
        
        if field_col and len(fields_to_lookup) > 0:
            field_placeholders = ','.join([':field' + str(i) for i in range(len(fields_to_lookup))])
            field_filter = f"AND {field_col} IN ({field_placeholders})"
            field_params = {f'field{i}': field for i, field in enumerate(fields_to_lookup)}
        
        # For large datasets, we'll do this in batches
        batch_size = 50  # Smaller batches due to field filtering
        all_results = []
        
        for i in range(0, len(coord_records), batch_size):
            batch = coord_records.iloc[i:i+batch_size]
            
            # Build coordinate-based query with tolerance
            coord_conditions = []
            params = {}
            
            for idx, (_, row) in enumerate(batch.iterrows()):
                lat = row['latitude']
                lon = row['longitude']
                
                coord_conditions.append(f"""
                (ABS({latitude_col} - :lat{idx}) < :tol{idx} AND 
                 ABS({longitude_col} - :lon{idx}) < :tol{idx})
                """)
                
                params[f'lat{idx}'] = lat
                params[f'lon{idx}'] = lon
                params[f'tol{idx}'] = tolerance
            
            # Add field parameters to params
            params.update(field_params)
            
            # Include spud date and field in the query if available
            spud_date_select = f", {spud_date_col} as SPUD_DATE" if spud_date_col else ", NULL as SPUD_DATE"
            field_select = f", {field_col} as FIELD" if field_col else ", NULL as FIELD"
            
            query = f"""
            SELECT DISTINCT {license_col} as LICENSE_NUMBER,
                           {latitude_col} as LATITUDE,
                           {longitude_col} as LONGITUDE,
                           {well_name_col} as WELL_NAME{spud_date_select}{field_select}
            FROM GDC.WELL 
            WHERE ({' OR '.join(coord_conditions)})
            AND {license_col} IS NOT NULL
            {field_filter}
            """
            
            try:
                batch_results = pd.read_sql(query, self.connection, params=params)
                all_results.append(batch_results)
                
            except Exception as e:
                logger.error(f"❌ Error in coordinate lookup batch {i//batch_size + 1}: {e}")
        
        if all_results:
            final_results = pd.concat(all_results, ignore_index=True)
            logger.info(f"✅ Found {len(final_results)} license matches by coordinates (field-filtered)")
            return final_results
        else:
            return pd.DataFrame()
    
    def match_and_update_licenses(self, missing_df: pd.DataFrame, column_mapping: Dict[str, str]) -> Dict:
        """Perform license lookup and create update recommendations"""
        
        results = {
            'well_name_matches': pd.DataFrame(),
            'coordinate_matches': pd.DataFrame(),
            'summary': {}
        }
        
        # Try well name matching first
        logger.info("🔍 Phase 1: Well name matching...")
        well_name_matches = self.lookup_licenses_by_well_name(missing_df, column_mapping)
        results['well_name_matches'] = well_name_matches
        
        # Try coordinate matching for remaining records
        logger.info("🗺️  Phase 2: Coordinate matching...")
        coordinate_matches = self.lookup_licenses_by_coordinates(missing_df, column_mapping)
        results['coordinate_matches'] = coordinate_matches
        
        # Create update recommendations
        update_recommendations = []
        
        # Process well name matches (both exact and fuzzy)
        if not well_name_matches.empty:
            for _, match in well_name_matches.iterrows():
                # For exact matches, use WELL_NAME directly
                # For fuzzy matches, use BIT_WELL_NAME to find the original bit file well name
                search_well_name = match.get('BIT_WELL_NAME', match['WELL_NAME'])
                
                matching_records = missing_df[missing_df['well_name'] == search_well_name]
                for _, record in matching_records.iterrows():
                    # Determine confidence level based on match type and similarity
                    if match.get('match_type') == 'exact':
                        confidence = 'high'
                        match_method = 'well_name_exact'
                    elif match.get('match_type') == 'fuzzy':
                        similarity = match.get('similarity', 0.0)
                        if similarity >= 0.95:
                            confidence = 'high'
                        elif similarity >= 0.85:
                            confidence = 'medium'
                        else:
                            confidence = 'low'
                        match_method = f'well_name_fuzzy_{similarity:.2f}'
                    else:
                        confidence = 'medium'
                        match_method = 'well_name'
                    
                    update_recommendations.append({
                        'original_index': record.name,
                        'well_name': record['well_name'],
                        'gdc_well_name': match['WELL_NAME'],
                        'operator': record['operator'],
                        'field': record['field'],
                        'found_license': match['LICENSE_NUMBER'],
                        'match_method': match_method,
                        'confidence': confidence,
                        'similarity_score': match.get('similarity', 1.0)
                    })
        
        # Process coordinate matches (for records not already matched by name)
        if not coordinate_matches.empty:
            # Get wells that were already matched by name (both exact and fuzzy)
            already_matched_wells = set()
            if not well_name_matches.empty:
                # For exact matches, use WELL_NAME; for fuzzy matches, use BIT_WELL_NAME
                for _, match in well_name_matches.iterrows():
                    if match.get('match_type') == 'fuzzy' and 'BIT_WELL_NAME' in match:
                        already_matched_wells.add(match['BIT_WELL_NAME'])
                    else:
                        already_matched_wells.add(match['WELL_NAME'])
            
            for _, record in missing_df.iterrows():
                if record['well_name'] in already_matched_wells:
                    continue  # Skip if already matched by name
                
                if pd.notna(record['latitude']) and pd.notna(record['longitude']):
                    # Find matches within coordinate tolerance
                    coord_diffs = np.sqrt(
                        (coordinate_matches['LATITUDE'] - record['latitude'])**2 + 
                        (coordinate_matches['LONGITUDE'] - record['longitude'])**2
                    )
                    
                    # Filter matches within tolerance
                    within_tolerance = coord_diffs < 0.001
                    candidate_matches = coordinate_matches[within_tolerance].copy()
                    
                    if len(candidate_matches) > 0:
                        # Calculate confidence scores for each candidate
                        best_match = None
                        best_confidence = 0.0
                        best_coord_diff = float('inf')
                        best_spud_info = {}
                        
                        for idx, match in candidate_matches.iterrows():
                            coord_diff = coord_diffs.loc[idx]
                            
                            # Base confidence from coordinate proximity (closer = higher confidence)
                            coord_confidence = max(0.1, 1.0 - (coord_diff / 0.001))
                            
                            # Add spud date confidence if available
                            spud_confidence = 0.0
                            spud_days_diff = -1
                            
                            if 'SPUD_DATE' in match and pd.notna(match['SPUD_DATE']) and 'spud_date' in record and pd.notna(record['spud_date']):
                                spud_confidence, spud_days_diff = self.calculate_spud_date_confidence(
                                    record['spud_date'], match['SPUD_DATE']
                                )
                            
                            # Combined confidence: coordinate match is primary, spud date adds boost
                            if spud_confidence > 0:
                                # If spud dates match well, boost overall confidence
                                combined_confidence = coord_confidence + (spud_confidence * 0.3)  # 30% boost from spud date
                                match_method = 'coordinates_and_spud_date'
                            else:
                                combined_confidence = coord_confidence
                                match_method = 'coordinates'
                            
                            # Track the best match
                            if combined_confidence > best_confidence:
                                best_confidence = combined_confidence
                                best_match = match
                                best_coord_diff = coord_diff
                                best_spud_info = {
                                    'spud_confidence': spud_confidence,
                                    'spud_days_diff': spud_days_diff,
                                    'match_method': match_method
                                }
                        
                        # Add the best match if confidence is reasonable
                        if best_match is not None and best_confidence >= 0.1:
                            confidence_level = 'high' if best_confidence >= 0.8 else 'medium' if best_confidence >= 0.5 else 'low'
                            
                            recommendation = {
                                'original_index': record.name,
                                'well_name': record['well_name'],
                                'operator': record['operator'],
                                'field': record['field'],
                                'found_license': best_match['LICENSE_NUMBER'],
                                'match_method': best_spud_info['match_method'],
                                'confidence': confidence_level,
                                'confidence_score': best_confidence,
                                'coord_difference': best_coord_diff
                            }
                            
                            # Add spud date information if available
                            if best_spud_info['spud_days_diff'] >= 0:
                                recommendation['spud_days_difference'] = best_spud_info['spud_days_diff']
                                recommendation['spud_confidence'] = best_spud_info['spud_confidence']
                            
                            update_recommendations.append(recommendation)
        
        results['update_recommendations'] = pd.DataFrame(update_recommendations)
        
        # Calculate enhanced summary statistics
        spud_date_matches = len([r for r in update_recommendations if 'spud_days_difference' in r and r['spud_days_difference'] >= 0])
        coordinate_only_matches = len([r for r in update_recommendations if r['match_method'] == 'coordinates'])
        coordinate_spud_matches = len([r for r in update_recommendations if r['match_method'] == 'coordinates_and_spud_date'])
        
        # Well name match type breakdown
        exact_well_name_matches = len([r for r in update_recommendations if r['match_method'] == 'well_name_exact'])
        fuzzy_well_name_matches = len([r for r in update_recommendations if 'well_name_fuzzy' in r['match_method']])
        total_well_name_matches = exact_well_name_matches + fuzzy_well_name_matches
        
        # Summary statistics
        results['summary'] = {
            'total_missing': len(missing_df),
            'well_name_matches_total': len(well_name_matches),
            'exact_well_name_matches': exact_well_name_matches,
            'fuzzy_well_name_matches': fuzzy_well_name_matches,
            'coordinate_matches': len(coordinate_matches),
            'total_recommendations': len(update_recommendations),
            'coordinate_only_matches': coordinate_only_matches,
            'coordinate_and_spud_matches': coordinate_spud_matches,
            'spud_date_enhanced_matches': spud_date_matches,
            'match_rate': len(update_recommendations) / len(missing_df) * 100 if len(missing_df) > 0 else 0
        }
        
        return results

def main():
    """Main function to perform GDC license lookup"""
    
    print("🚀 GDC Oracle Database License Lookup")
    print("=" * 50)
    
    lookup = GDCLicenseLookup()
    
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
        
        # Show sample data
        print(f"\n📋 Sample GDC.WELL data:")
        print("-" * 30)
        print(sample_df.head())
        
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
        
        # Perform license lookup
        print(f"\n🔍 Starting license lookup process...")
        results = lookup.match_and_update_licenses(missing_df, column_mapping)
        
        # Display results
        print(f"\n📊 LOOKUP RESULTS:")
        print("=" * 50)
        print(f"Total missing licenses: {results['summary']['total_missing']:,}")
        print(f"Well name matches found: {results['summary']['well_name_matches_total']:,}")
        print(f"  ├─ Exact matches: {results['summary']['exact_well_name_matches']:,}")
        print(f"  └─ Fuzzy matches: {results['summary']['fuzzy_well_name_matches']:,}")
        print(f"Coordinate matches found: {results['summary']['coordinate_matches']:,}")
        print(f"Update recommendations: {results['summary']['total_recommendations']:,}")
        print(f"  ├─ Coordinate only: {results['summary']['coordinate_only_matches']:,}")
        print(f"  ├─ Coordinate + spud date: {results['summary']['coordinate_and_spud_matches']:,}")
        print(f"  └─ With spud date validation: {results['summary']['spud_date_enhanced_matches']:,}")
        print(f"Success rate: {results['summary']['match_rate']:.1f}%")
        
        # Save results
        output_dir = Path("Output")
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        
        if not results['update_recommendations'].empty:
            recommendations_file = output_dir / f"License_Lookup_Results_{timestamp}.xlsx"
            
            with pd.ExcelWriter(recommendations_file) as writer:
                results['update_recommendations'].to_excel(writer, sheet_name='Recommendations', index=False)
                if not results['well_name_matches'].empty:
                    results['well_name_matches'].to_excel(writer, sheet_name='Well_Name_Matches', index=False)
                if not results['coordinate_matches'].empty:
                    results['coordinate_matches'].to_excel(writer, sheet_name='Coordinate_Matches', index=False)
            
            print(f"\n💾 Results saved to: {recommendations_file.name}")
        else:
            print("\n❌ No license matches found")
        
    except Exception as e:
        logger.error(f"❌ Error in main process: {e}")
        
    finally:
        lookup.disconnect()

if __name__ == "__main__":
    main()
