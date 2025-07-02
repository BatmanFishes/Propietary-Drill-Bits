import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import glob

def auto_merge_ulterra_files():
    """
    Automatically merge all Excel files in the Input/Ulterra folder.
    This script is designed for automated/scheduled runs.
    """
    
    # Define paths
    base_path = Path(__file__).parent
    input_folder = base_path / "Input" / "Ulterra"
    output_folder = base_path / "Output"
    
    # Create output folder if it doesn't exist
    output_folder.mkdir(exist_ok=True)
    
    # Log file for tracking runs
    log_file = output_folder / "merge_log.txt"
    
    def log_message(message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        print(message)  # Also print to console
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
    
    log_message("=== Starting Ulterra File Merge ===")
    
    # Check if input folder exists
    if not input_folder.exists():
        log_message(f"ERROR: Input folder {input_folder} does not exist!")
        return False
    
    # Find all Excel files in the Ulterra folder
    excel_files = list(input_folder.glob("*.xlsx"))
    
    if not excel_files:
        log_message("WARNING: No Excel files found in the Ulterra folder!")
        return False
    
    log_message(f"Found {len(excel_files)} Excel files to merge")
    
    # Check if we need to merge (compare with last run)
    last_run_file = output_folder / "last_merge_files.txt"
    current_files_info = {}
    
    for file_path in excel_files:
        mod_time = file_path.stat().st_mtime
        current_files_info[file_path.name] = mod_time
    
    # Check if files have changed since last run
    files_changed = True
    if last_run_file.exists():
        try:
            with open(last_run_file, "r") as f:
                last_files_info = eval(f.read())
            
            if current_files_info == last_files_info:
                files_changed = False
                log_message("No file changes detected since last merge. Skipping merge.")
        except:
            log_message("Could not read last run information. Proceeding with merge.")
    
    if not files_changed:
        return True
    
    # Dictionary to store dataframes from each file
    all_data = {}
    successful_files = 0
    
    # Process each Excel file
    for file_path in excel_files:
        try:
            log_message(f"Processing: {file_path.name}")
            
            # Read the Excel file
            excel_data = pd.ExcelFile(file_path)
            sheet_names = excel_data.sheet_names
            
            # Store data from each sheet
            file_data = {}
            for sheet_name in sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Add source file information
                df['Source_File'] = file_path.name
                df['File_Modified'] = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                file_data[sheet_name] = df
                log_message(f"  Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
            
            all_data[file_path.stem] = file_data
            successful_files += 1
            
        except Exception as e:
            log_message(f"ERROR processing {file_path.name}: {str(e)}")
            continue
    
    if not all_data:
        log_message("ERROR: No data was successfully loaded from any files!")
        return False
    
    log_message(f"Successfully processed {successful_files} out of {len(excel_files)} files")
    
    # Create merged datasets
    merged_data = {}
    
    # Get all unique sheet names across all files
    all_sheet_names = set()
    for file_data in all_data.values():
        all_sheet_names.update(file_data.keys())
    
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
        
        log_message(f"SUCCESS: Merged file saved as: {output_filename}")
        
        # Print summary statistics
        total_rows = sum(len(df) for df in merged_data.values())
        log_message(f"Summary: {len(merged_data)} sheets, {total_rows} total rows")
        
        # Save current file information for next run
        with open(last_run_file, "w") as f:
            f.write(str(current_files_info))
        
        log_message("=== Merge completed successfully ===")
        return True
        
    except Exception as e:
        log_message(f"ERROR saving merged file: {str(e)}")
        return False

if __name__ == "__main__":
    success = auto_merge_ulterra_files()
    exit(0 if success else 1)
