import pandas as pd

# Load both datasets
original = pd.read_excel('Output/Integrated_BitData_20250702_102059.xlsx')
updated = pd.read_excel('Output/Integrated_BitData_SafeUpdated_20250702_112148.xlsx')

print('VERIFICATION OF SAFE UPDATES:')
print('=' * 32)
print(f'Original missing licenses: {original["license_number"].isna().sum()}')
print(f'Updated missing licenses: {updated["license_number"].isna().sum()}')

licenses_added = original["license_number"].isna().sum() - updated["license_number"].isna().sum()
print(f'Licenses added: {licenses_added}')
print(f'Success rate: {(licenses_added / original["license_number"].isna().sum())*100:.1f}%')

print('\nSample of wells that got license numbers:')
diff_mask = original['license_number'].isna() & updated['license_number'].notna()
sample = updated[diff_mask][['well_name', 'license_number', 'operator']].head(10)
print(sample.to_string())
