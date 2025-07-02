#!/usr/bin/env python3
"""
Simplified GDC lookup test with detailed debugging
"""

import pandas as pd
import oracledb
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load missing license data
def load_missing_data():
    file_path = Path("Output/Integrated_BitData_20250626_155052.xlsx")
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return None
    
    df = pd.read_excel(file_path)
    missing = df[
        (df['data_source'] == 'ulterra') &
        ((df['license_number'].isna()) | (df['license_number'] == ''))
    ].copy()
    
    logger.info(f"Found {len(missing)} records missing license numbers")
    return missing.head(10)  # Just test with first 10

# Test coordinate lookup
def test_coordinate_lookup(sample_df):
    try:
        # Connect to Oracle
        dsn = "gdcprod.whitecapres.corp:1521/gdcprod.whitecapres.corp"
        connection = oracledb.connect(
            user="Colin.Anderson",
            password="Colanderson1981",
            dsn=dsn
        )
        logger.info("✅ Connected to Oracle database")
        
        # Test with very loose coordinate matching (±0.1 degrees)
        for idx, row in sample_df.iterrows():
            well_name = row['well_name']
            lat, lng = row['latitude'], row['longitude']
            field = row['field']
            
            logger.info(f"\n🔍 Testing: {well_name}")
            logger.info(f"   Field: {field}")
            logger.info(f"   Coordinates: {lat:.6f}, {lng:.6f}")
            
            # Very simple coordinate query with wide tolerance
            query = f"""
            SELECT WELL_NUM, WELL_NAME, SURFACE_LATITUDE, SURFACE_LONGITUDE, ASSIGNED_FIELD
            FROM GDC.WELL
            WHERE SURFACE_LATITUDE BETWEEN {lat - 0.1} AND {lat + 0.1}
            AND SURFACE_LONGITUDE BETWEEN {lng - 0.1} AND {lng + 0.1}
            AND WELL_NUM IS NOT NULL
            ORDER BY ABS(SURFACE_LATITUDE - {lat}) + ABS(SURFACE_LONGITUDE - {lng})
            FETCH FIRST 5 ROWS ONLY
            """
            
            try:
                results = pd.read_sql(query, connection)
                logger.info(f"   Found {len(results)} potential matches:")
                
                for _, match in results.iterrows():
                    dist_lat = abs(match['SURFACE_LATITUDE'] - lat)
                    dist_lng = abs(match['SURFACE_LONGITUDE'] - lng)
                    logger.info(f"     {match['WELL_NUM']} | {match['WELL_NAME'][:30]} | Field: {match['ASSIGNED_FIELD']} | Δ: {dist_lat:.4f}, {dist_lng:.4f}")
                    
            except Exception as e:
                logger.error(f"   Error: {e}")
        
        connection.close()
        logger.info("✅ Disconnected from database")
        
    except Exception as e:
        logger.error(f"❌ Connection error: {e}")

if __name__ == "__main__":
    logger.info("🧪 Starting simplified lookup test...")
    
    sample_data = load_missing_data()
    if sample_data is not None:
        test_coordinate_lookup(sample_data)
    else:
        logger.error("Could not load sample data")
