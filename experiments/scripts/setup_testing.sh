cd $HOME
mkdir data
cd data
mkdir generation
cd generation
mkdir ycsb
mkdir kvbench
mkdir tectonic

# Get all dirs with ls command
for dir in $(ls); do
  for scale in 1x 10x 100x; do
    mkdir -p "$dir/$scale"
    if [ "$dir" = "ycsb" ]; then
      for workload in a b c d e f; do
        mkdir -p "$dir/$scale/$workload"
      done
    elif [ "$dir" = "kvbench" ]; then
      for workload in i ii iii iv v vi vii viii ix x xi xii xiii xiv xv xvi xvii xviii xix xx xxi xxii xxiii xxiv; do
        mkdir -p "$dir/$scale/$workload"
      done
    elif [ "$dir" = "tectonic" ]; then
      for workload in a b c d e f; do
        mkdir -p "$dir/$scale/ycsb/$workload"
      done
      for workload in i ii iii iv v vi vii viii ix x xi xii xiii xiv xv xvi xvii xviii xix xx xxi xxii xxiii xxiv; do
        mkdir -p "$dir/$scale/kvbench/$workload"
      done
    fi
  done
done

cd $HOME/data
mkdir benchmarking
cd benchmarking
mkdir tectonic
mkdir ycsb

# Get all dirs with ls command
for dir in $(ls); do
  for backend in rocksdb cassandra redis s3; do
    for scale in 1x 10x 100x; do
      if [ "$dir" = "ycsb" ]; then
        for workload in a b c d e f; do
          mkdir -p "$dir/$backend/$scale/$workload"
        done
      elif [ "$dir" = "tectonic" ]; then
        for workload in a b c d e f; do
          mkdir -p "$dir/$backend/$scale/ycsb/$workload"
        done
      fi
    done
  done
done
