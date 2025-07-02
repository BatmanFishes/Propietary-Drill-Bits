import sys
sys.path.append('.')
from data_mapping_config import DataMappingConfig

config = DataMappingConfig()

print('Configured Data Sources:')
print('=' * 25)
for name, source in config.data_sources.items():
    print(f'{name}: {source.description}')
    print(f'  Folder: {source.folder_path}')
    print(f'  Pattern: {source.file_pattern}')
    print(f'  Sheet: {source.sheet_name}')
    print()
