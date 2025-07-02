"""
Quick script to check field names in bit data and compare with GDC fields
"""

import pandas as pd
from pathlib import Path

# Load the integrated data
output_dir = Path("Output")
integrated_files = list(output_dir.glob("Integrated_BitData_*.xlsx"))
latest_file = max(integrated_files, key=lambda x: x.stat().st_mtime)

print(f"Loading: {latest_file.name}")
df = pd.read_excel(latest_file)

# Filter to Ulterra records missing license numbers
missing_license = df[
    (df['data_source'] == 'ulterra') &
    ((df['license_number'].isna()) | (df['license_number'] == ''))
].copy()

print(f"Records missing license: {len(missing_license)}")

# Check field names
fields_in_bit_data = missing_license[missing_license['field'].notna()]['field'].unique()
print(f"\nFields in bit data ({len(fields_in_bit_data)}):")
for field in sorted(fields_in_bit_data):
    print(f"  - {field}")

# Sample a few well names to see their format
sample_wells = missing_license[missing_license['well_name'].notna()]['well_name'].head(10).tolist()
print(f"\nSample well names:")
for well in sample_wells:
    print(f"  - {well}")

# Check if coordinates are available
coords_available = len(missing_license[
    missing_license['latitude'].notna() & 
    missing_license['longitude'].notna()
])
print(f"\nRecords with coordinates: {coords_available}")

# Sample coordinates
coord_sample = missing_license[
    missing_license['latitude'].notna() & 
    missing_license['longitude'].notna()
][['well_name', 'field', 'latitude', 'longitude']].head(5)

print(f"\nSample coordinates:")
print(coord_sample.to_string(index=False))
