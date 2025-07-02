#!/usr/bin/env python3
"""
Diagnostic script to understand why the GDC lookup isn't finding matches
"""

import pandas as pd
import oracledb
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GDCDiagnostic:
    def __init__(self):
        self.connection = None
        
    def connect_to_oracle(self):
        """Connect to Oracle GDC database"""
        try:
            # Use the same connection method as the main script
            dsn = "gdcprod.whitecapres.corp:1521/gdcprod.whitecapres.corp"
            self.connection = oracledb.connect(
                user="Colin.Anderson",
                password="Colanderson1981",
                dsn=dsn
            )
            logger.info("✅ Connected to Oracle GDC database")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Oracle: {e}")
            return False
    
    def load_sample_bit_data(self):
        """Load a small sample of bit data to analyze"""
        bit_file = Path("Integrated_BitData_20250626_155052.xlsx")
        if not bit_file.exists():
            logger.error(f"❌ Bit data file not found: {bit_file}")
            return None
            
        df = pd.read_excel(bit_file)
        missing = df[
            (df['data_source'] == 'ulterra') &
            ((df['license_number'].isna()) | (df['license_number'] == ''))
        ].copy()
        
        # Take first 5 records for analysis
        sample = missing.head(5)
        logger.info(f"📊 Loaded {len(sample)} sample bit records")
        return sample
    
    def analyze_coordinate_ranges(self, bit_sample):
        """Analyze coordinate ranges in bit data vs GDC"""
        logger.info("🗺️ Analyzing coordinate ranges...")
        
        # Bit data coordinates
        logger.info("📍 Bit data coordinate sample:")
        for idx, row in bit_sample.iterrows():
            logger.info(f"  {row['well_name']}: {row['latitude']:.6f}, {row['longitude']:.6f}")
        
        # GDC coordinate ranges
        coord_query = """
        SELECT 
            MIN(SURFACE_LATITUDE) as min_lat,
            MAX(SURFACE_LATITUDE) as max_lat,
            MIN(SURFACE_LONGITUDE) as min_lng,
            MAX(SURFACE_LONGITUDE) as max_lng,
            COUNT(*) as total_wells
        FROM GDC.WELL 
        WHERE SURFACE_LATITUDE IS NOT NULL 
        AND SURFACE_LONGITUDE IS NOT NULL
        """
        
        try:
            coord_df = pd.read_sql(coord_query, self.connection)
            logger.info("📊 GDC coordinate ranges:")
            logger.info(f"  Latitude: {coord_df['MIN_LAT'].iloc[0]:.6f} to {coord_df['MAX_LAT'].iloc[0]:.6f}")
            logger.info(f"  Longitude: {coord_df['MIN_LNG'].iloc[0]:.6f} to {coord_df['MAX_LNG'].iloc[0]:.6f}")
            logger.info(f"  Total wells with coordinates: {coord_df['TOTAL_WELLS'].iloc[0]:,}")
        except Exception as e:
            logger.error(f"❌ Error querying GDC coordinates: {e}")
    
    def test_coordinate_lookup(self, bit_sample):
        """Test coordinate lookup with different tolerances"""
        logger.info("🎯 Testing coordinate lookup with different tolerances...")
        
        for tolerance in [0.1, 0.05, 0.01, 0.005]:
            logger.info(f"  Testing tolerance: ±{tolerance} degrees")
            
            sample_row = bit_sample.iloc[0]
            lat, lng = sample_row['latitude'], sample_row['longitude']
            
            test_query = f"""
            SELECT COUNT(*) as match_count
            FROM GDC.WELL
            WHERE SURFACE_LATITUDE BETWEEN {lat - tolerance} AND {lat + tolerance}
            AND SURFACE_LONGITUDE BETWEEN {lng - tolerance} AND {lng + tolerance}
            """
            
            try:
                result = pd.read_sql(test_query, self.connection)
                count = result['MATCH_COUNT'].iloc[0]
                logger.info(f"    Found {count} potential matches")
            except Exception as e:
                logger.error(f"    Error: {e}")
    
    def analyze_field_names(self, bit_sample):
        """Analyze field name patterns"""
        logger.info("🏷️ Analyzing field names...")
        
        # Bit data fields
        unique_fields = bit_sample['field'].unique()
        logger.info(f"📍 Bit data fields: {list(unique_fields)}")
        
        # Sample GDC field assignments
        field_query = """
        SELECT ASSIGNED_FIELD, COUNT(*) as count
        FROM GDC.WELL
        WHERE ASSIGNED_FIELD IS NOT NULL
        GROUP BY ASSIGNED_FIELD
        ORDER BY count DESC
        FETCH FIRST 20 ROWS ONLY
        """
        
        try:
            field_df = pd.read_sql(field_query, self.connection)
            logger.info("📊 Top GDC assigned fields:")
            for _, row in field_df.iterrows():
                logger.info(f"  {row['ASSIGNED_FIELD']}: {row['COUNT']} wells")
        except Exception as e:
            logger.error(f"❌ Error querying GDC fields: {e}")
        
        # Check well names containing field patterns
        for field in unique_fields[:3]:  # Check first 3 fields
            if pd.notna(field):
                well_name_query = f"""
                SELECT COUNT(*) as count
                FROM GDC.WELL
                WHERE UPPER(WELL_NAME) LIKE '%{field.upper()}%'
                """
                
                try:
                    result = pd.read_sql(well_name_query, self.connection)
                    count = result['COUNT'].iloc[0]
                    logger.info(f"  Wells with '{field}' in name: {count}")
                except Exception as e:
                    logger.error(f"  Error checking '{field}': {e}")
    
    def test_well_name_matching(self, bit_sample):
        """Test well name matching"""
        logger.info("🔤 Testing well name matching...")
        
        for idx, row in bit_sample.head(3).iterrows():
            well_name = row['well_name']
            logger.info(f"  Testing well: {well_name}")
            
            # Exact match
            exact_query = f"""
            SELECT WELL_NAME, WELL_NUM
            FROM GDC.WELL
            WHERE UPPER(WELL_NAME) = '{well_name.upper()}'
            """
            
            try:
                exact_df = pd.read_sql(exact_query, self.connection)
                logger.info(f"    Exact matches: {len(exact_df)}")
                if len(exact_df) > 0:
                    logger.info(f"    Found: {exact_df['WELL_NAME'].iloc[0]} -> {exact_df['WELL_NUM'].iloc[0]}")
            except Exception as e:
                logger.error(f"    Exact match error: {e}")
            
            # Partial match
            partial_query = f"""
            SELECT WELL_NAME, WELL_NUM
            FROM GDC.WELL
            WHERE UPPER(WELL_NAME) LIKE '%{well_name.upper()[:10]}%'
            FETCH FIRST 5 ROWS ONLY
            """
            
            try:
                partial_df = pd.read_sql(partial_query, self.connection)
                logger.info(f"    Partial matches: {len(partial_df)}")
                for _, match in partial_df.iterrows():
                    logger.info(f"      {match['WELL_NAME']} -> {match['WELL_NUM']}")
            except Exception as e:
                logger.error(f"    Partial match error: {e}")
    
    def run_diagnostics(self):
        """Run all diagnostics"""
        logger.info("🔍 Starting GDC lookup diagnostics...")
        
        if not self.connect_to_oracle():
            return
        
        try:
            # Load sample data
            bit_sample = self.load_sample_bit_data()
            if bit_sample is None:
                return
            
            # Run diagnostics
            self.analyze_coordinate_ranges(bit_sample)
            self.test_coordinate_lookup(bit_sample)
            self.analyze_field_names(bit_sample)
            self.test_well_name_matching(bit_sample)
            
        finally:
            if self.connection:
                self.connection.close()
                logger.info("🔌 Disconnected from Oracle database")

if __name__ == "__main__":
    diagnostic = GDCDiagnostic()
    diagnostic.run_diagnostics()
