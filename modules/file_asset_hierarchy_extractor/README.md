# Asset Hierarchy Creation from Files

A comprehensive solution for extracting asset tags from diagram files and creating hierarchical asset structures in Cognite Data Fusion (CDF).

## 🎯 What This Solution Does

1. **Extracts asset tags** from diagram files (PDFs, DWG, etc.) using pattern matching
2. **Creates hierarchical structure** based on your organization's locations (sites, plants, areas, systems)
3. **Writes assets** to CDF data modeling for use in your applications

## 🚀 Quick Start

1. **Read the Getting Started Guide**: Start with [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md)
2. **Copy the Example**: Use [`config.simple.example.yaml`](config.simple.example.yaml) as your starting point
3. **Configure Your Locations**: Add your sites, plants, areas, systems, and files
4. **Configure Your Patterns**: Define what asset tags to extract
5. **Run the Pipeline**: Execute the workflow to create your hierarchy

## 📚 Documentation

- **[Getting Started Guide](docs/GETTING_STARTED.md)** - Quick start for new users
- **[Configuration Guide](docs/CONFIGURATION_GUIDE.md)** - Detailed configuration options
- **[Summary of Improvements](docs/SUMMARY_OF_IMPROVEMENTS.md)** - What's been optimized
- **[Optimization Plan](docs/OPTIMIZATION_PLAN.md)** - Technical details

## 📁 Configuration Files

### Configuration Examples
- `config.simple.example.yaml` - Working example configuration (quick start)

### Industry-Specific Templates
Choose the template that matches your industry:
- `config.template.manufacturing.yaml` - **Manufacturing**: Site → Plant → Area → System
- `config.template.oil_gas.yaml` - **Oil & Gas**: Site → Facility → Unit → System
- `config.template.utilities.yaml` - **Utilities**: Region → Site → Building → Room → System
- `config.template.pharmaceuticals.yaml` - **Pharmaceuticals**: Site → Building → Suite → System

### Pipeline Configurations
- `pipelines/ctx_extract_assets_by_pattern_default.config.yaml` - Asset extraction configuration
- `pipelines/ctx_create_asset_hierarchy_default.config.yaml` - Hierarchy creation configuration
- `pipelines/ctx_write_asset_hierarchy_default.config.yaml` - Asset writing configuration

## 🏗️ Solution Architecture

### Functions

1. **Extract Assets by Pattern** (`fn_dm_extract_assets_by_pattern`)
   - Processes diagram files
   - Extracts asset tags using pattern matching
   - Stores results in RAW tables

2. **Create Asset Hierarchy** (`fn_dm_create_asset_hierarchy`)
   - Reads extracted assets
   - Creates hierarchical structure based on locations
   - Generates asset instances with proper relationships

3. **Write Asset Hierarchy** (`fn_dm_write_asset_hierarchy`)
   - Reads generated hierarchy
   - Writes assets to CDF data modeling
   - Handles batch processing and updates

### Workflow

The solution includes a CDF workflow that orchestrates all three functions:
- `workflows/file_asset_hierarchy_extractor.Workflow.yaml`

## ⚙️ Configuration Structure

### Business Configuration (What You Customize)

```yaml
# Your hierarchy structure
hierarchy:
  levels: [site, plant, area, system]

# Your locations and files
locations:
  - name: "YOUR_SITE"
    description: "Site Description"
    locations:
      # ... nested structure
      - name: "YOUR_SYSTEM"
        files: ["File-001", "File-002"]

# What asset tags to extract
patterns:
  - category: equipment
    samples: ["P-101", "V-201"]
```

### Technical Configuration (Usually Leave as Default)

```yaml
# Storage settings
storage:
  database: db_extract_assets_by_pattern
  tables:
    results: extract_assets_by_pattern_results
    assets: extract_assets_by_pattern_assets

# Processing settings
processing:
  batch_size: 5
  max_attempts: 3
  limit: -1  # -1 = all files
```

## 💡 Key Features

### Flexible Hierarchy
- **Customizable levels**: Define your own hierarchy structure (e.g., site/plant/area/system or facility/building/room)
- **Dynamic naming**: Hierarchy level names come from configuration
- **Files at any level**: Support for files defined at any hierarchy level

### Pattern Matching
- **Flexible patterns**: Use `[X]` for any letter, `X` for any letters, numbers for digits
- **Multiple categories**: Equipment, instruments, documents, general
- **Resource classification**: Optional resourceType and resourceSubType

### User-Friendly Configuration
- **Clear separation**: Technical settings vs business configuration
- **Inline documentation**: Every setting explained
- **Examples included**: Simple and complex examples provided
- **Validation ready**: Structure supports validation

## 🔧 For Data Engineers

### Code Organization
- **Shared utilities**: Common code in `functions/shared/utils/`
- **Modular functions**: Each function is self-contained
- **Consistent patterns**: Similar structure across all functions

### Configuration Management
- **Pipeline configs**: YAML-based configuration
- **Environment variables**: For CDF connection
- **Local testing**: Scripts for local execution

## 👥 For Partners/Non-Technical Users

### Easy Configuration
- **Step-by-step guides**: Clear instructions
- **Template files**: Copy and modify
- **Examples**: Working examples to follow

### Clear Documentation
- **Getting started**: Quick start guide
- **Configuration guide**: Detailed options
- **Troubleshooting**: Common issues and solutions

## 📖 Common Use Cases

### Use Case 1: Simple 3-Level Hierarchy
```yaml
hierarchy:
  levels: [facility, building, room]

locations:
  - name: "Main Facility"
    locations:
      - name: "Building A"
        locations:
          - name: "Control Room"
            files: ["Diagram-001"]
```

### Use Case 2: Complex Multi-Site
```yaml
hierarchy:
  levels: [region, site, unit, system]

locations:
  - name: "North Region"
    locations:
      - name: "Site Alpha"
        locations:
          - name: "Unit 1"
            locations:
              - name: "Cooling System"
                files: ["CW-001", "CW-002"]
```

## 🛠️ Running Locally

See individual function directories for local execution scripts:
- `run_extract_assets_by_pattern.py`
- `run_create_asset_hierarchy.py`
- `run_write_asset_hierarchy.py`

## 📝 Notes

- **Backward Compatible**: Existing configurations still work
- **Gradual Migration**: Can adopt new structure gradually
- **Validation**: Configuration validation with helpful errors (coming soon)

## 🤝 Contributing

When making changes:
1. Update documentation if configuration changes
2. Add examples for new features
3. Keep user-friendly comments in configs
4. Test with simple examples

## 📄 License

[Add license information]
