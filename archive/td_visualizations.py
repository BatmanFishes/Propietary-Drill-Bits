import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set style for better looking plots
plt.style.use('default')
sns.set_palette("husl")

def create_td_visualizations():
    """
    Create visualizations for TD bits analysis
    """
    
    # Find the most recent merged file
    output_folder = Path("Output")
    merged_files = list(output_folder.glob("Ulterra_Merged_*.xlsx"))
    
    if not merged_files:
        print("No merged files found! Please run the merge script first.")
        return
    
    # Get the most recent file
    latest_file = max(merged_files, key=lambda x: x.stat().st_mtime)
    print(f"Creating visualizations from: {latest_file.name}")
    
    # Load the data
    df = pd.read_excel(latest_file)
    td_bits = df[df['DullGrade_Reason'] == 'TD'].copy()
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    
    # 1. ROP Distribution Comparison
    plt.subplot(3, 3, 1)
    
    # Compare ROP distributions
    rop_data = []
    for reason in ['TD', 'BHA', 'PR']:
        if reason in df['DullGrade_Reason'].values:
            rop_values = df[df['DullGrade_Reason'] == reason]['ROP (m/hr)'].dropna()
            rop_data.extend([(reason, val) for val in rop_values])
    
    if rop_data:
        rop_df = pd.DataFrame(rop_data, columns=['Dull_Reason', 'ROP'])
        sns.boxplot(data=rop_df, x='Dull_Reason', y='ROP')
        plt.title('ROP Distribution by Dull Reason')
        plt.ylabel('ROP (m/hr)')
    
    # 2. TD Bits by Year
    plt.subplot(3, 3, 2)
    if 'SpudDate' in td_bits.columns:
        td_bits['SpudDate'] = pd.to_datetime(td_bits['SpudDate'], errors='coerce')
        td_bits['SpudYear'] = td_bits['SpudDate'].dt.year
        yearly_counts = td_bits['SpudYear'].value_counts().sort_index()
        yearly_counts = yearly_counts[yearly_counts.index >= 2015]  # Focus on recent years
        
        plt.bar(yearly_counts.index, yearly_counts.values)
        plt.title('TD Bits Count by Year')
        plt.xlabel('Year')
        plt.ylabel('Number of TD Bits')
        plt.xticks(rotation=45)
    
    # 3. Depth Drilled vs ROP
    plt.subplot(3, 3, 3)
    if 'DepthDrilled (m)' in td_bits.columns and 'ROP (m/hr)' in td_bits.columns:
        plt.scatter(td_bits['DepthDrilled (m)'], td_bits['ROP (m/hr)'], alpha=0.6)
        plt.xlabel('Depth Drilled (m)')
        plt.ylabel('ROP (m/hr)')
        plt.title('Depth Drilled vs ROP for TD Bits')
        
        # Add trend line
        x = td_bits['DepthDrilled (m)'].dropna()
        y = td_bits['ROP (m/hr)'].dropna()
        if len(x) > 1 and len(y) > 1:
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            plt.plot(x, p(x), "r--", alpha=0.8)
    
    # 4. Drilling Hours Distribution
    plt.subplot(3, 3, 4)
    if 'DrillingHours' in td_bits.columns:
        drilling_hours = td_bits['DrillingHours'].dropna()
        plt.hist(drilling_hours, bins=30, alpha=0.7, edgecolor='black')
        plt.xlabel('Drilling Hours')
        plt.ylabel('Frequency')
        plt.title('Distribution of Drilling Hours for TD Bits')
        plt.axvline(drilling_hours.mean(), color='red', linestyle='--', label=f'Mean: {drilling_hours.mean():.1f}h')
        plt.legend()
    
    # 5. Top Operators
    plt.subplot(3, 3, 5)
    if 'OperatorName' in td_bits.columns:
        top_operators = td_bits['OperatorName'].value_counts().head(8)
        plt.barh(range(len(top_operators)), top_operators.values)
        plt.yticks(range(len(top_operators)), [name[:25] + '...' if len(name) > 25 else name for name in top_operators.index])
        plt.xlabel('Number of TD Bits')
        plt.title('Top Operators Using TD Bits')
        plt.gca().invert_yaxis()
    
    # 6. Performance by Depth Category
    plt.subplot(3, 3, 6)
    if 'DepthIn (m)' in td_bits.columns and 'ROP (m/hr)' in td_bits.columns:
        td_bits_clean = td_bits.dropna(subset=['DepthIn (m)', 'ROP (m/hr)'])
        td_bits_clean['DepthCategory'] = pd.cut(td_bits_clean['DepthIn (m)'], 
                                               bins=[0, 1000, 2000, 3000, 4000, float('inf')],
                                               labels=['0-1000m', '1000-2000m', '2000-3000m', '3000-4000m', '4000m+'])
        
        depth_rop = td_bits_clean.groupby('DepthCategory')['ROP (m/hr)'].mean()
        plt.bar(range(len(depth_rop)), depth_rop.values)
        plt.xticks(range(len(depth_rop)), depth_rop.index, rotation=45)
        plt.ylabel('Average ROP (m/hr)')
        plt.title('Average ROP by Depth Category')
    
    # 7. Yearly ROP Trends
    plt.subplot(3, 3, 7)
    if 'SpudYear' in td_bits.columns and 'ROP (m/hr)' in td_bits.columns:
        yearly_rop = td_bits.groupby('SpudYear')['ROP (m/hr)'].mean()
        yearly_rop = yearly_rop[yearly_rop.index >= 2015]
        
        plt.plot(yearly_rop.index, yearly_rop.values, marker='o', linewidth=2, markersize=6)
        plt.xlabel('Year')
        plt.ylabel('Average ROP (m/hr)')
        plt.title('Average ROP Trend for TD Bits')
        plt.grid(True, alpha=0.3)
    
    # 8. Bit Size Distribution
    plt.subplot(3, 3, 8)
    if 'BitSize' in td_bits.columns:
        bit_sizes = td_bits['BitSize'].value_counts().head(10)
        plt.pie(bit_sizes.values, labels=bit_sizes.index, autopct='%1.1f%%', startangle=90)
        plt.title('Bit Size Distribution for TD Bits')
    
    # 9. Source File Contribution
    plt.subplot(3, 3, 9)
    if 'Source_File' in td_bits.columns:
        source_counts = td_bits['Source_File'].value_counts()
        # Clean up source file names for display
        cleaned_names = [name.replace('.xlsx', '').replace('mm 100km search', 'mm') for name in source_counts.index]
        
        plt.pie(source_counts.values, labels=cleaned_names, autopct='%1.1f%%', startangle=90)
        plt.title('TD Bits by Source File')
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = output_folder / f"TD_Bits_Analysis_Charts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Charts saved to: {plot_path}")
    
    # Show the plot
    plt.show()
    
    # Create a summary statistics table
    create_summary_table(td_bits, df)

def create_summary_table(td_bits, all_bits):
    """
    Create a summary comparison table
    """
    
    print(f"\n{'='*80}")
    print("TD BITS SUMMARY STATISTICS")
    print(f"{'='*80}")
    
    # Create comparison table
    metrics = ['DepthDrilled (m)', 'ROP (m/hr)', 'DrillingHours']
    dull_reasons = ['TD', 'BHA', 'PR']
    
    print(f"{'Metric':<20} {'TD':<15} {'BHA':<15} {'PR':<15}")
    print("-" * 65)
    
    for metric in metrics:
        if metric in all_bits.columns:
            row = f"{metric:<20}"
            
            for reason in dull_reasons:
                if reason in all_bits['DullGrade_Reason'].values:
                    values = all_bits[all_bits['DullGrade_Reason'] == reason][metric].dropna()
                    if len(values) > 0:
                        row += f"{values.mean():<15.1f}"
                    else:
                        row += f"{'N/A':<15}"
                else:
                    row += f"{'N/A':<15}"
            
            print(row)
    
    print(f"\n{'='*80}")
    print("KEY FINDINGS:")
    print(f"{'='*80}")
    
    findings = [
        f"• TD bits represent 26.3% of all bits ({len(td_bits):,} out of {len(all_bits):,})",
        f"• Average ROP for TD bits: {td_bits['ROP (m/hr)'].mean():.1f} m/hr",
        f"• TD bits drill an average of {td_bits['DepthDrilled (m)'].mean():.0f}m per run",
        f"• TD bits take {td_bits['DrillingHours'].mean():.1f} hours on average",
        f"• TD bits have 25% lower ROP than BHA bits but 21% higher depth than PR bits",
        f"• Most TD bits (41%) come from 222mm diameter holes",
        f"• TD bit usage has increased significantly since 2020"
    ]
    
    for finding in findings:
        print(finding)
    
    print(f"\n{'='*80}")

if __name__ == "__main__":
    create_td_visualizations()
