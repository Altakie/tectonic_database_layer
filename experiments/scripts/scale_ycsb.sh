cd 1x
for file in $(ls); do
  scale=1000
  perl -i -pe "s/(record_count)([0-9]+)/\$1 . \$2*$scale/e" $file
  perl -i -pe "s/(op_count)([0-9]+)/\$1 . \$2*$scale/e" $file
done
cd ..
# Get all dirs with ls command
for scale in 10 100; do
  cp 1x/* ${scale}x
  cd ${scale}x
  for file in $(ls); do
    perl -i -pe "s/(record_count)([0-9]+)/\$1 . \$2*$scale/e" $file
    perl -i -pe "s/(op_count)([0-9]+)/\$1 . \$2*$scale/e" $file
  done
  cd ..
done
