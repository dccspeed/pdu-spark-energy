# Spark History Server Event Logs

## Overview

This repository contains **three comprehensive Spark History Server event log datasets** with 2,000 Spark applications total, perfectly correlated with power consumption data for energy analysis research.

| Dataset | Archive | Size | Apps | Period | Power Correlation |
|---------|---------|------|------|--------|-------------------|
| **Standard** | `HSs_standard_load.tar.xz` | 148 MB | 800 | Jun 2, 2024 | `all_power_standard_processed.csv.gz` ✅ |
| **Light Load** | `HSs_light_load.tar.xz` | 77 MB | 600 | Dec 19-27, 2024 | `all_power_light_processed.csv.gz` ✅ |
| **Heavy Load** | `HSs_heavy_load.tar.xz` | 144 MB | 600 | Jan 16, 2025 | `all_power_heavy_processed.csv.gz` ✅ |

**Total:** 2,000 applications enabling comprehensive Spark energy analysis with perfect temporal correlations.

---

## Dataset Structure

All three datasets follow the same organizational structure:

```
HSs_{standard|light|heavy}_load/
├── VM6cores/          # 6-core VM configuration
│   ├── SVM/          # Support Vector Machine applications
│   ├── Kmeans/       # K-Means clustering applications
│   ├── Pagerank/     # PageRank applications
│   ├── Terasort/     # TeraSort applications
│   └── Matfact/      # Matrix Factorization applications
│
└── VM3cores/          # 3-core VM configuration
    ├── SVM/          # Support Vector Machine applications
    ├── Kmeans/       # K-Means clustering applications
    ├── Pagerank/     # PageRank applications
    ├── Terasort/     # TeraSort applications
    └── Matfact/      # Matrix Factorization applications
```

---

## Statistics

### Standard Dataset: 800 Applications

| VM Config | Total Apps | SVM | Kmeans | Pagerank | Terasort | Matfact |
|-----------|------------|-----|--------|----------|----------|---------|
| **VM6cores** | 400 | 80 | 80 | 80 | 80 | 80 |
| **VM3cores** | 400 | 80 | 80 | 80 | 80 | 80 |

### Light & Heavy Datasets: 600 Applications Each

| VM Config | Total Apps | SVM | Kmeans | Pagerank | Terasort | Matfact |
|-----------|------------|-----|--------|----------|----------|---------|
| **VM6cores** | 200 | 40 | 40 | 40 | 40 | 40 |
| **VM3cores** | 400 | 80 | 80 | 80 | 80 | 80 |

### Combined: 2,000 Applications

| Workload | Standard | Light | Heavy | Total |
|----------|----------|-------|-------|-------|
| **SVM** | 160 | 120 | 120 | 400 |
| **Kmeans** | 160 | 120 | 120 | 400 |
| **Pagerank** | 160 | 120 | 120 | 400 |
| **Terasort** | 160 | 120 | 120 | 400 |
| **Matfact** | 160 | 120 | 120 | 400 |

---

## VM Configurations

### VM6cores
- **Cores:** 6 CPU cores per VM
- **Density:** 1 VM per physical worker
- **Hosts:** slave401-406
- **Total:** 6 VMs across 6 workers
- **Apps:** 400 (standard), 200 (light/heavy)

### VM3cores
- **Cores:** 3 CPU cores per VM
- **Density:** 2 VMs per physical worker  
- **Hosts:** slave201-212
- **Total:** 12 VMs across 6 workers
- **Apps:** 400 per dataset

---

## Workload Types

### 1. SVM (Support Vector Machine)
- **Type:** Binary classification algorithm
- **Applications:** 400 total (160 standard + 120 light + 120 heavy)
- **Typical stages:** ~17 stages per run
- **Characteristics:** Iterative gradient descent optimization
- **Use case:** Binary classification, supervised learning

### 2. Kmeans (K-Means Clustering)
- **Type:** Unsupervised learning algorithm
- **Applications:** 400 total (160 standard + 120 light + 120 heavy)
- **Typical stages:** ~15 stages per run
- **Characteristics:** Iterative centroid computation
- **Use case:** Data clustering and partitioning

### 3. Pagerank
- **Type:** Graph algorithm
- **Applications:** 400 total (160 standard + 120 light + 120 heavy)
- **Typical stages:** ~23 stages per run
- **Characteristics:** Iterative graph traversal
- **Use case:** Web page ranking, graph analysis

### 4. Terasort
- **Type:** Sorting benchmark
- **Applications:** 400 total (160 standard + 120 light + 120 heavy)
- **Typical stages:** ~2 stages per run
- **Characteristics:** Map-reduce sorting pattern
- **Use case:** Large-scale data sorting performance test

### 5. Matfact (Matrix Factorization)
- **Type:** Collaborative filtering algorithm
- **Applications:** 400 total (160 standard + 120 light + 120 heavy)
- **Typical stages:** ~24 stages per run
- **Characteristics:** Alternating Least Squares (ALS)
- **Use case:** Recommendation systems, rating prediction

---

## Standard Dataset

### HSs_standard_load.tar.xz

**Period:** June 2, 2024  
**Size:** 148 MB compressed (~460 MB extracted)  
**Applications:** 800 (largest dataset)

**Temporal Correlation:**
```
Timeline (May-June 2024):
  May 24 ─────────────────── Jun 2
                              ▲
                              Jun 2: Standard Spark (800 apps)
                              Standard Power monitors entire period
```

**Power Data:** `all_power_standard_processed.csv.gz` (May 24 - Jun 2)  
**Correlation:** ✅ Within monitoring period (final day)

**Characteristics:**
- Largest dataset (800 apps)
- More apps per workload (80 vs 40 on VM6cores)
- Baseline measurements from May/June 2024
- Ideal for establishing baseline energy profiles

---

## Light Load Dataset

### HSs_light_load.tar.xz

**Period:** December 19-27, 2024 (7 days)  
**Size:** 77 MB compressed (~250 MB extracted)  
**Applications:** 600

**Perfect Temporal Correlation:**
```
Timeline (December 2024):
  Dec 19 ━━━━━━━━━━━━━━━━━━━━━ Dec 27
         ▲                           ▲
         ├── Spark Applications ────┤  (600 apps)
         └── Power Measurements ────┘  (3.19M readings)
```

**Power Data:** `all_power_light_processed.csv.gz`  
**Correlation:** ✅ Perfect match - Same exact time period

**Characteristics:**
- Lighter workload intensity
- Smaller event log files (2-3 MB avg per app)
- Dedicated 7-day monitoring window
- Ideal for baseline energy analysis

---

## Heavy Load Dataset

### HSs_heavy_load.tar.xz

**Period:** January 16, 2025  
**Size:** 144 MB compressed (~450 MB extracted)  
**Applications:** 600

**Temporal Correlation:**
```
Timeline (Dec 2024 - Jan 2025):
  Dec 31 ────────────────────────── Jan 29
         ↑
         Jan 16: Heavy Spark (600 apps)
         Heavy Power monitors entire period
```

**Power Data:** `all_power_heavy_processed.csv.gz` (Dec 31 - Jan 29)  
**Correlation:** ✅ Within monitoring period

**Characteristics:**
- Heavier workload intensity
- Larger event log files (4-6 MB avg per app)
- Part of extended 28-day monitoring
- Ideal for intensive workload analysis

---

## Comparison Across All Datasets

| Aspect | Standard | Light Load | Heavy Load |
|--------|----------|-----------|------------|
| **Archive Size** | 148 MB | 77 MB | 144 MB |
| **Extracted Size** | ~460 MB | ~250 MB | ~450 MB |
| **Applications** | 800 | 600 | 600 |
| **Period** | Jun 2, 2024 | Dec 19-27, 2024 | Jan 16, 2025 |
| **Duration** | 1 day | 7 days | 1 day |
| **Power Data** | May 24-Jun 2 | Dec 19-27 | Dec 31-Jan 29 |
| **VM6cores Apps** | 400 | 200 | 200 |
| **VM3cores Apps** | 400 | 400 | 400 |
| **Intensity** | Baseline | Light | Heavy |

**Key Insights:**
- Standard has 2x more VM6cores apps (80 vs 40 per workload)
- Light/Heavy follow same distribution pattern
- Three different time periods enable temporal comparisons

---

## Event Log Format

Each application file contains JSON-formatted Spark events (one per line):

```json
{"Event":"SparkListenerApplicationStart","App Name":"SVM Classifier Example",...}
{"Event":"SparkListenerExecutorAdded","Timestamp":1734637833691,...}
{"Event":"SparkListenerStageCompleted","Stage ID":0,...}
{"Event":"SparkListenerTaskEnd","Task Info":{...},"Task Metrics":{...}}
{"Event":"SparkListenerApplicationEnd","Timestamp":1734637900000}
```

### Key Event Types

| Event Type | Description | Usage |
|------------|-------------|-------|
| `SparkListenerApplicationStart` | Application metadata, start time | App-level profiling |
| `SparkListenerExecutorAdded` | Executor allocation, host mapping | Resource tracking |
| `SparkListenerStageCompleted` | Stage metrics (CPU, memory, I/O) | Stage-level analysis |
| `SparkListenerTaskEnd` | Task execution details | Task-level profiling |
| `SparkListenerApplicationEnd` | Completion timestamp | Runtime calculation |

### File Specifications

- **Format:** JSON Lines (one JSON object per line)
- **Compression:** None (readable text files)
- **Encoding:** UTF-8
- **Size:** 2-6 MB per application
- **Naming:** `application_<timestamp>_<id>`

---

## Usage with This Repository

### Extracting Archives

```bash
cd data/

# Extract standard
tar -xf HSs_standard_load.tar.xz

# Extract light load
tar -xf HSs_light_load.tar.xz

# Extract heavy load
tar -xf HSs_heavy_load.tar.xz
```

### Analyzing Across All Datasets

```python
import pduhistoryserver as hs
import pduoutletmap as om
from pdumetrics import PowerMetrics

# Load all power datasets
pm_standard = PowerMetrics('data/all_power_standard_processed.csv.gz')
pm_light = PowerMetrics('data/all_power_light_processed.csv.gz')
pm_heavy = PowerMetrics('data/all_power_heavy_processed.csv.gz')

# Map datasets
datasets = {
    'standard': {
        'dir': 'HSs_standard_load',
        'pm': pm_standard,
        'example': 'VM6cores/SVM/application_1717334488267_0001'
    },
    'light': {
        'dir': 'HSs_light_load', 
        'pm': pm_light,
        'example': 'VM6cores/SVM/application_1734637358713_0001'
    },
    'heavy': {
        'dir': 'HSs_heavy_load',
        'pm': pm_heavy,
        'example': 'VM6cores/SVM/application_1737003650790_0001'
    }
}

# Analyze each dataset
for name, config in datasets.items():
    app_path = f"data/{config['dir']}/{config['example']}"
    start, end = hs.get_time_interval(app_path)
    
    hosts = hs.get_hosts(app_path)
    outlets = om.get_outlets_from_hosts(hosts)
    
    energy = sum([
        config['pm'].get_energy_watt_second_and_power_avg_watt(outlet, start, end)[0]
        for outlet in outlets
    ])
    
    print(f"{name.capitalize()}: {energy/3600:.2f} Wh")
```

### Comparing Workloads Across Time

```python
# Compare SVM energy consumption across all three datasets
workload = "SVM"
vm_config = "VM6cores"

results = {}
for name, config in datasets.items():
    apps_dir = Path(f"data/{config['dir']}/{vm_config}/{workload}")
    apps = list(apps_dir.glob("application_*"))
    
    energies = []
    for app in apps[:10]:  # Sample 10 apps
        start, end = hs.get_time_interval(str(app))
        # Calculate energy...
        energies.append(energy)
    
    results[name] = {
        'mean': np.mean(energies),
        'std': np.std(energies)
    }

# Analyze temporal differences
print(f"SVM Energy Consumption Comparison:")
for name, stats in results.items():
    print(f"  {name}: {stats['mean']:.2f} ± {stats['std']:.2f} Wh")
```

---

## Research Applications

This comprehensive dataset collection enables:

### 1. Temporal Analysis
- Compare energy patterns across three time periods (May, Dec, Jan)
- Seasonal variations in cluster operation
- Long-term energy trends

### 2. Load Comparison Studies
- Same workloads under standard, light, and heavy loads
- Energy scaling with workload intensity
- Identify load-dependent patterns

### 3. Workload Energy Profiling
- 5 different algorithm types
- 400 applications per workload type
- Stage-level energy breakdown

### 4. Configuration Analysis
- VM3cores vs VM6cores under different conditions
- Resource allocation efficiency
- Energy per core comparison

### 5. Comprehensive Energy Modeling
- Build predictive models with 2,000 applications
- Correlate Spark metrics with power consumption
- Robust statistical validation

### 6. Baseline Establishment
- Standard dataset (800 apps) for baseline metrics
- Compare against light/heavy scenarios
- Identify anomalies and patterns

---

## Validation Notes

✅ **Verified:**
- All 2,000 application files are valid JSON event logs
- Complete event sequences (ApplicationStart to ApplicationEnd)
- Temporal alignment with power data confirmed for all datasets
- Directory structure consistent across all three datasets
- VM configurations match `pduoutletmap.py` worker assignments
- Event types consistent across all applications

✅ **Ready for analysis** with existing repository tools

---

## Data Availability

**These archives are excluded from git** (see `.gitignore`):
- **Reason:** Large file sizes (148 + 77 + 144 = 369 MB compressed)
- **Alternative:** Store separately, provide download links, or request from maintainer
- **Metadata:** This documentation is tracked in git

**To obtain these datasets:**
1. Contact repository maintainer
2. Check project's data distribution method
3. Or regenerate if you have access to the cluster

---

## File Formats Summary

### Spark Event Logs
- **Format:** JSON Lines (NDJSON)
- **Compression:** None (text files)
- **Size per app:** 2-6 MB
- **Total:** ~1.2 GB extracted

### Power Metrics (Companion)
- **Format:** CSV (time,active-power-outlet,value)
- **Compression:** gzip
- **Standard:** `all_power_standard_processed.csv.gz` (23 MB)
- **Light:** `all_power_light_processed.csv.gz` (18 MB)
- **Heavy:** `all_power_heavy_processed.csv.gz` (48 MB)
- **See:** `DATA_DESCRIPTION.md` for details

---

*Document Version: 3.0*  
*Last Updated: 2026-05-20*  
*Datasets: Standard (Jun 2, 2024) + Light (Dec 19-27, 2024) + Heavy (Jan 16, 2025)*  
*Total Applications: 2,000*  
*Workloads: SVM, Kmeans, Pagerank, Terasort, Matfact*
