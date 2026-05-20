# PDU Spark Energy Analysis

A toolkit for analyzing Apache Spark application energy consumption using Power Distribution Unit (PDU) metrics.

## Overview

This project correlates Spark application execution data with power consumption measurements from PDUs. It processes Spark History Server event logs and PDU power metrics to analyze energy usage at the stage and application level.

### Key Features

- Process raw PDU power metrics from InfluxDB JSON format
- Extract Spark stage-level execution profiles
- Correlate power consumption with Spark stages
- Generate timeline visualizations of power usage
- Support for multi-worker cluster configurations

### Project Structure

- `pdumetrics.py` - Core power metrics processing and energy calculations
- `pduhistoryserver.py` - Spark History Server event log parser
- `pduoutletmap.py` - Maps physical workers to PDU outlets
- `pduplots.py` - Visualization utilities for power timelines
- `pduutil.py` - Helper functions for file processing
- `generate_processed_power_metrics_file.py` - CLI tool for processing raw PDU data
- `stage_level_regression.ipynb` - Example notebook for data analysis

## Setup

### 1. Create Python Environment and Install Dependencies

From the project directory:

```bash
python3 -m venv venv                  # Create a local python environment
source venv/bin/activate              # Activate local env (all packages installed in venv/)
pip install -r requirements.txt       # Install required packages
```

### 2. Generate Processed Power Metrics from InfluxDB JSON

Convert raw PDU data (InfluxDB JSON export) to processed CSV format:

```bash
python generate_processed_power_metrics_file.py allset_power_metrics.csv.gz
```

**Note:** Input file should be a GZIP-compressed file (not tar.gz). Output is saved as `all_power_processed.csv.gz`.

### 3. Organize History Server Files (Optional)

If using Spark History Server event logs, organize them according to the expected directory structure:

```bash
./mv_files.sh
```

This script generates `history-server-tree.json`, which contains the recursive file structure expected by analysis scripts.

## Usage

### Jupyter Notebooks

Launch Jupyter Lab for interactive data analysis:

```bash
jupyter lab
```

Start with `stage_level_regression.ipynb` for an example that:
- Loads formatted stages dataframe
- Shows extracted metrics
- Demonstrates correlation analysis between stages and energy consumption

### Python API

See individual module documentation for programmatic usage.

## Requirements

- Python 3.7+
- pandas, numpy, matplotlib
- scikit-learn, scipy
- jupyter, jupyterlab

See `requirements.txt` for complete dependency list.

## Data Files

**Note:** Large data files (*.csv.gz) are excluded from version control.

If you cloned this repository and need the power measurement datasets:
- See `data/DATA_DESCRIPTION.md` for dataset descriptions
- Contact the repository maintainer for access to data files
- Or regenerate from raw sources using `python preprocess_data.py`

The metadata files (`data/metadata.json` and `data/DATA_DESCRIPTION.md`) are included in the repository for reference.
