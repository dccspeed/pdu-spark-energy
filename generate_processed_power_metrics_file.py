import sys
import pduutil as pu

if __name__ == '__main__':
    power_metrics_file_path_gz = sys.argv[1]
    pu.generate_processed_power_metrics_file(power_metrics_file_path_gz)
