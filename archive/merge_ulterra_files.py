import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import glob

def merge_ulterra_files():
    """
    Merge all Excel files in the Input/Ulterra folder.
    This script finds the most recent version of files and merges them into a single Excel file.
    """
    
    # Define paths
    base_path = Path(__file__).parent
    input_folder = base_path / "Input" / "Ulterra"
    output_folder = base_path / "Output"
    
    # Create output folder if it doesn't exist
    output_folder.mkdir(exist_ok=True)
    
    # Check if input folder exists
    if not input_folder.exists():
        print(f"Error: Input folder {input_folder} does not exist!")
        return
    
    # Find all Excel files in the Ulterra folder
    excel_files = list(input_folder.glob("*.xlsx"))
    
    if not excel_files:
        print("No Excel files found in the Ulterra folder!")
        return
    
    print(f"Found {len(excel_files)} Excel files:")
    for file in excel_files:
        print(f"  - {file.name}")
    
    # Dictionary to store dataframes from each file
    all_data = {}
    
    # Process each Excel file
    for file_path in excel_files:
        try:
            print(f"\nProcessing: {file_path.name}")
            
            # Read the Excel file
            # First, try to read all sheets to see the structure
            excel_data = pd.ExcelFile(file_path)
            sheet_names = excel_data.sheet_names
            print(f"  Sheets found: {sheet_names}")
            
            # Store data from each sheet
            file_data = {}
            for sheet_name in sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                print(f"    Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
                
                # Add source file information
                df['Source_File'] = file_path.name
                df['File_Modified'] = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                file_data[sheet_name] = df
            
            all_data[file_path.stem] = file_data
            
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
            continue
    
    if not all_data:
        print("No data was successfully loaded from any files!")
        return
    
    # Create merged datasets
    merged_data = {}
    
    # Get all unique sheet names across all files
    all_sheet_names = set()
    for file_data in all_data.values():
        all_sheet_names.update(file_data.keys())
    
    print(f"\nMerging data from sheets: {list(all_sheet_names)}")
    
    # Merge data by sheet name
    for sheet_name in all_sheet_names:
        sheet_dataframes = []
        
        for file_name, file_data in all_data.items():
            if sheet_name in file_data:
                sheet_dataframes.append(file_data[sheet_name])
        
        if sheet_dataframes:
            # Concatenate all dataframes for this sheet
            merged_df = pd.concat(sheet_dataframes, ignore_index=True, sort=False)
            merged_data[sheet_name] = merged_df
            print(f"  Sheet '{sheet_name}': {len(merged_df)} total rows after merging")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"Ulterra_Merged_{timestamp}.xlsx"
    output_path = output_folder / output_filename
    
    # Write merged data to Excel file
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet_name, df in merged_data.items():
                # Clean sheet name (Excel sheet names have character limits and restrictions)
                clean_sheet_name = sheet_name[:31]  # Excel limit is 31 characters
                clean_sheet_name = clean_sheet_name.replace('/', '_').replace('\\', '_')
                
                df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                print(f"  Written sheet: {clean_sheet_name}")
        
        print(f"\nMerged file saved as: {output_path}")
        print(f"Total sheets: {len(merged_data)}")
        
        # Print summary statistics
        total_rows = sum(len(df) for df in merged_data.values())
        print(f"Total rows across all sheets: {total_rows}")
        
    except Exception as e:
        print(f"Error saving merged file: {str(e)}")

def get_file_info():
    """
    Display information about files in the Ulterra folder without merging.
    """
    base_path = Path(__file__).parent
    input_folder = base_path / "Input" / "Ulterra"
    
    if not input_folder.exists():
        print(f"Error: Input folder {input_folder} does not exist!")
        return
    
    excel_files = list(input_folder.glob("*.xlsx"))
    
    if not excel_files:
        print("No Excel files found in the Ulterra folder!")
        return
    
    print("Ulterra Excel Files Information:")
    print("=" * 50)
    
    for file_path in sorted(excel_files):
        mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        file_size = file_path.stat().st_size / 1024  # Size in KB
        
        print(f"\nFile: {file_path.name}")
        print(f"  Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Size: {file_size:.1f} KB")
        
        try:
            excel_data = pd.ExcelFile(file_path)
            print(f"  Sheets: {len(excel_data.sheet_names)} - {excel_data.sheet_names}")
            
            for sheet_name in excel_data.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                print(f"    '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
                
        except Exception as e:
            print(f"  Error reading file: {str(e)}")

if __name__ == "__main__":
    print("Ulterra File Merger")
    print("=" * 30)
    
    # Ask user what they want to do
    print("\nOptions:")
    print("1. View file information only")
    print("2. Merge all files")
    
    try:
        choice = input("\nEnter your choice (1 or 2): ").strip()
        
        if choice == "1":
            get_file_info()
        elif choice == "2":
            merge_ulterra_files()
        else:
            print("Invalid choice. Running merge by default...")
            merge_ulterra_files()
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
