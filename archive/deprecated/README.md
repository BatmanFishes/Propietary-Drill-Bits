# Deprecated GDC Lookup Scripts

These scripts have been replaced by the comprehensive GDC enhancement module in the main pipeline.

## Replaced Files:
- `gdc_license_lookup.py` - Standard GDC lookup (less strict matching)
- `gdc_license_lookup_safe.py` - Safe GDC lookup (strict matching)

## Why Deprecated:
The new approach (`core/gdc_enhancement.py`) is superior because it:

1. **Comprehensive Coverage**: Processes ALL records instead of just missing ones
2. **Better Matching**: Uses trimmed license number matching (more reliable than well name/location)
3. **Authoritative Data**: Updates license numbers with GDC's WELL_NUM (preserving proper leading zeros)
4. **Integrated Workflow**: Runs automatically in the main pipeline (no manual steps)
5. **Complete Enhancement**: Updates license_number, uwi_number, AND uwi_formatted fields

## Migration:
- The new system is integrated into `universal_data_integration.py`
- Enhancement happens automatically during the main integration process
- Detailed reports are generated in the Output folder
- No user action required - the old manual lookup steps are eliminated

Date Deprecated: 2025-07-07
Reason: Replaced by comprehensive integrated solution
