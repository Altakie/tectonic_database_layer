#!/bin/bash
workloads=(
  "a"
  "b"
  "c"
  "d"
  "e"
  "f"
)
time="/usr/bin/time"
ycsb="$HOME/YCSB/bin/ycsb.sh"
stats="$HOME/data/benchmarking/ycsb/rocksdb/1x"
runs=5
spec_path="$HOME/workload_specs/ycsb/1x"
rocksdb_path="/tmp/ycsb-rocksdb-data"

monitor_ycsb() {
  local cpu_log_path="$1"
  local parent_pid="$2"

  local current_pid=""
  local monitor_pid=""
  while kill -0 $parent_pid 2>/dev/null; do
    new_pid=$(pgrep java 2>/dev/null)
    if [[ -n "$new_pid" && "$new_pid" != "$current_pid" ]]; then
      [[ -n "$monitor_pid" ]] && kill $monitor_pid 2>/dev/null
      current_pid=$new_pid
      pidstat 1 -h -r -u -p $current_pid >>"$cpu_log_path" &
      monitor_pid=$!
    fi
    sleep 0.1
  done
  [[ -n "$monitor_pid" ]] && kill $monitor_pid 2>/dev/null
}

mkdir -p $stats

for wl in "${workloads[@]}"; do
  for run in $(seq 1 $runs); do
    prefix="${wl}/run${run}"
    log_path="$stats/$prefix.out"
    time_path="$stats/$prefix.time"
    cpu_log_path="$stats/$prefix.cpu"

    rm -rf $rocksdb_path
    mkdir -p $rocksdb_path
    sudo sysctl -w vm.drop_caches=3

    cmd="$ycsb load rocksdb -P $spec_path/workload${wl} -p rocksdb.dir=$rocksdb_path && $ycsb run rocksdb -P $spec_path/workload${wl} -p rocksdb.dir=$rocksdb_path"

    $time -v -o $time_path bash -c "$cmd" >> >(tee -a $log_path) &
    pid="$!"

    sleep 0.1
    monitor_ycsb "$cpu_log_path" "$pid" &
    monitor_pid=$!

    wait $pid
    exit_code=$?
    kill $monitor_pid 2>/dev/null

    if [[ $exit_code -ne 0 ]]; then
      echo "Something went wrong: workload $wl run $run failed." | tee -a "$log_path"
    else
      echo ">>> Finished $wl run $run"
    fi
    echo
  done
done
