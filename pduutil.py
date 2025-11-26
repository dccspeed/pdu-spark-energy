import json
import gzip

def datafiles_iterator():
    with open("history-server-tree.json", "r") as f:
        directory_tree = json.load(f)

    directory_tree = directory_tree[0]
    base_dir = directory_tree["name"]
    for app_tree in directory_tree["contents"]:
        app_dir = app_tree["name"]
        for vmconfig_tree in app_tree["contents"]:
            vmconfig_dir = vmconfig_tree["name"]
            for num_machines_tree in vmconfig_tree["contents"]:
                num_machines_dir = num_machines_tree["name"]
                for datafile_tree in num_machines_tree["contents"]:
                    datafile = datafile_tree["name"]
                    datafile_full_path = f"{base_dir}/{app_dir}/{vmconfig_dir}/{num_machines_dir}/{datafile}"
                    config = {
                        "app": app_dir,
                        "vm_config": vmconfig_dir,
                        "num_machines": num_machines_dir,
                        "app_id": datafile,
                    }
                    yield datafile_full_path, config


def generate_processed_power_metrics_file(power_metrics_file_path):
    with gzip.open(power_metrics_file_path, 'r') as pmfile, \
            gzip.open("all_power_processed.csv.gz", 'w') as pmfile_processed:
        header = b"time,active-power-outlet,value\n"
        pmfile_processed.write(header)
        values = json.load(pmfile)["results"][0]["series"][0]["values"]
        for v in values:
            if v[2] == "power" and "active-power-outlet" in v[3]:
                outlet = int(v[3].split("-")[3])
                power = int(v[4])
                encoded = "%s,%d,%d\n" % (v[0],outlet, power)
                encoded = str.encode(encoded)
                pmfile_processed.write(encoded)

