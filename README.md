# Manual

## Create python environment and install dependencies

From ```pdu-nestor/``` directory:

```
python3 -m venv venv # create a local python env
source venv/bin/activate # activate local env (all packages are installed in venv/)
pip install -r requirements # install packages
```

## Generate processed power metrics from InfluxDB JSON

```
python python generate_processed_power_metrics_file.py allset_power_metrics.csv.gz
```

Obs. ```allset_power_metrics.csv.gz``` is a GZIP file (not tar.gz)

## Organize History Server files according to the expected directory structure

First download files to this directory.

```
./mv_files.sh
```

Obs. This also generates ```history-server-tree.json```, file with recursive file structure expected by the scripts.

## Use a notebook for loading existing data (to downstream analysis)

```
jupyter lab # opens browser tab for interacting with .ipynb files
```

Obs. Start with ```stage_level_regression.ipynb``` for an example that reads the formatted stages dataframe, including extracted metrics.
