# Whitecap Data Integration System

A production-ready data integration system for combining drilling data from multiple sources (Reed, Ulterra, GDC) with proper error handling, logging, and modular architecture.

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run full integration
python main.py integrate --all --output "Output/integrated_data.xlsx"

# Generate summary report
python main.py report --input "Output/integrated_data.xlsx" --template summary

# Analyze data quality
python main.py analyze --input "Output/integrated_data.xlsx" --verbose
```

## 📁 Project Structure

```
├── main.py                    # Main entry point with clean CLI
├── src/whitecap_integration/  # Core application code
│   ├── config/               # Configuration management
│   ├── data/                 # Data loading and processing
│   ├── integration/          # Integration engines
│   ├── reports/              # Report generation
│   └── utils/                # Utility functions
├── scripts/                  # Maintenance and processing scripts
│   ├── analysis/            # Data analysis scripts
│   ├── maintenance/         # System maintenance
│   └── processing/          # Data processing utilities
├── tests/                   # Test suite
├── docs/                    # Documentation
├── configs/                 # Configuration files
├── Input/                   # Input data files
└── Output/                  # Generated outputs
```

## 🔧 Configuration

The system uses YAML configuration files for easy maintenance:

- `configs/data_sources.yaml` - Data source configurations
- `configs/logging.yaml` - Logging configuration

## 📊 Supported Data Sources

- **Reed Data**: Well drilling records and completion data
- **Ulterra Data**: Bit performance and drilling optimization data  
- **GDC Data**: Geological and completion information

## 🎯 Key Features

- **Modular Architecture**: Clean separation of concerns
- **Robust Error Handling**: Comprehensive error handling and logging
- **Data Validation**: Built-in data quality checks
- **Flexible Configuration**: YAML-based configuration system
- **Multiple Output Formats**: Excel, CSV, JSON support
- **Comprehensive Reporting**: Summary, detailed, and audit reports
- **CLI Interface**: Easy-to-use command-line interface

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_integration.py -v

# Run with coverage
python -m pytest tests/ --cov=src/whitecap_integration
```

## 📚 Documentation

Detailed documentation is available in the `docs/` directory:

- [User Guide](docs/user_guide/README.md)
- [API Reference](docs/api/README.md)
- [Examples](docs/examples/README.md)

## 🔍 Development

### Adding New Data Sources

1. Create a new processor in `src/whitecap_integration/data/processors.py`
2. Add configuration in `configs/data_sources.yaml`
3. Update the integration engine in `src/whitecap_integration/integration/`
4. Add tests in `tests/`

### Contributing

1. Follow the existing code structure and patterns
2. Add comprehensive tests for new features
3. Update documentation as needed
4. Use meaningful commit messages

## 📝 License

Proprietary - Whitecap Resources Inc.

## 🆘 Support

For questions or issues, contact the Whitecap Data Engineering team.

---

**Version**: 2.0.0  
**Last Updated**: 2024-12-19  
**Author**: Whitecap Resources Data Engineering Team
