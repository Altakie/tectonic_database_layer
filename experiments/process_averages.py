import math
import csv


workloads = {"a": {}, "b": {}, "c": {}, "d": {}, "e": {}, "f": {}}

with open("averages.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        name = row["Workload"]
        wall_time = row["WallTime_sec"]
        throughput = row["Throughput_ops_sec"]
        memory = row["RSS_kb"]
        cpu = row["CPU_percent"]
        if "tectonic" in name:
            parts = name.split("ycsb")
            w_name = parts[len(parts) - 1]
            workloads[w_name]["t"] = {
                "wall_time": float(wall_time),
                "throughput": float(throughput),
                "memory": float(memory),
                "cpu": float(cpu),
            }
        elif "ycsb" in name:
            parts = name.split("ycsb_")
            w_name = parts[len(parts) - 1]

            workloads[w_name]["y"] = {
                "wall_time": float(wall_time),
                "throughput": float(throughput),
                "memory": float(memory),
                "cpu": float(cpu),
            }
            print(f"Ycsb {w_name}")


for workload, benches in workloads.items():
    ycsb = benches["y"]
    tectonic = benches["t"]
    t_keys = tectonic.keys()

    print(f"Workload {workload}")
    for key in t_keys:
        t = tectonic[key]
        y = ycsb[key]

        print(f"{key}: {t / y * 100:.2f}")
    print()
