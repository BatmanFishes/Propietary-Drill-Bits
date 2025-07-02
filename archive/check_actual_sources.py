import pandas as pd

df = pd.read_excel('Output/Integrated_BitData_SafeUpdated_20250702_112148.xlsx')

print('=== ACTUAL DATA SOURCES IN USE ===')
print(f'Total Records: {len(df):,}')
print(f'Data Sources: {list(df["data_source"].unique())}')
print()
print('Records by Source:')
print(df['data_source'].value_counts())
print()
print('Sample records from each source:')
for source in df['data_source'].unique():
    source_data = df[df['data_source'] == source]
    print(f'\n{source.upper()} (Sample):')
    print(f'  Records: {len(source_data):,}')
    print(f'  Fields: {list(source_data.columns[:10])}')
    if not source_data['spud_date'].isna().all():
        print(f'  Date range: {source_data["spud_date"].min()} to {source_data["spud_date"].max()}')
    else:
        print(f'  Date range: No spud dates available')
