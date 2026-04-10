# Get all dirs with ls command
for scale in 10 100; do
  cp 1x/* ${scale}x
  cd ${scale}x
  for file in $(ls):
    perl -i -pe "s/(\"op_count\": )([0-9]+)(,)/\$1 . \$2*$scale . \$3/e" file
  cd ..
done
