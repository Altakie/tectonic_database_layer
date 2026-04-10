#! /bin/bash
workloads=(
  "a"
  "b"
  "c"
  "d"
  "e"
  "f"
)

time="/usr/bin/time"
tectonic="$HOME/Tectonic/target/release/tectonic-cli"
db_path="/tmp/tectonic-rocksdb"
stats="$HOME/stats"
runs=5
spec_path="$HOME/tectonic_workloads/ycsb"
time=/usr/bin/time

cd $HOME/Tectonic
git switch no-marker-array
cargo build --release

for wl in "${workloads[@]}"; do
  for run in $(seq 1 $runs); do
    prefix="tectonic_ycsb${wl}_run${run}"
    log_path="$stats/$prefix.out"
    time_path="$stats/$prefix.time"
    cpu_log_path="$stats/$prefix.cpu"

    sudo sysctl -w vm.drop_caches=3

    $time -v -o $time_path $tectonic benchmark \
      -w $spec_path/$wl.spec.json -d rocksdb > >(tee $log_path) &
    pid="$!"

    sleep 0.1
    child_pid=$(pgrep -P $pid tectonic)
    while ! ps -p $child_pid >/dev/null 2>&1; do sleep 0.01; done

    pidstat 1 -h -r -u -p $child_pid >"$cpu_log_path" &
    monitor_pid=$!

    wait $pid
    exit_code=$?
    kill $monitor_pid 2>/dev/null

    if [[ $exit_code -ne 0 ]]; then
      echo "Something went wrong: workload $wl failed." | tee -a "$log_path"
    else
      echo ">>> Finished $wl Run $run"
    fi
    echo
  done
done

cd $HOME
