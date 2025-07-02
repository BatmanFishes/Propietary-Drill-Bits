import pandas as pd
from pathlib import Path

# Load the latest results
output_dir = Path('Output')
result_files = list(output_dir.glob('License_Lookup_Results_*.xlsx'))
latest_file = max(result_files, key=lambda x: x.stat().st_mtime)

print(f'Loading results from: {latest_file.name}')
df = pd.read_excel(latest_file, sheet_name='Recommendations')

print(f'\nFound {len(df)} recommendations:')
print('=' * 50)

for i, row in df.iterrows():
    print(f'\n{i+1}. Well: {row["well_name"]}')
    print(f'   Operator: {row["operator"]}')
    print(f'   Field: {row["field"]}')
    print(f'   Found License: {row["found_license"]}')
    print(f'   Match Method: {row["match_method"]}')
    print(f'   Confidence: {row["confidence"]}')
    if 'confidence_score' in row:
        print(f'   Confidence Score: {row["confidence_score"]:.3f}')
    print(f'   Coord Difference: {row["coord_difference"]:.6f}')
    if 'spud_days_difference' in row and pd.notna(row['spud_days_difference']):
        print(f'   Spud Days Diff: {row["spud_days_difference"]} days')
        if 'spud_confidence' in row:
            print(f'   Spud Confidence: {row["spud_confidence"]:.3f}')

# Show column information
print(f'\n\nAvailable columns in results:')
for col in df.columns:
    print(f'  {col}')
