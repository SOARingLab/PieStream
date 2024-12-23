#!/bin/bash

# Set environment variables and output directory
. ./env

#echo  $JAVA_CMD
OUT_DIR="out/latency_time"
mkdir -p $OUT_DIR

TIMESTAMP=$(date +"%m%d%H%M")
OUT_FILE="$OUT_DIR/latency_$TIMESTAMP.out"

echo -n > $OUT_FILE

cols=( 4 )
limits=(  1000000 )
windSize=( 10000 )
rates=( 1000000 500000  100000 50000  10000  5000 1000   )
loopNum=10

# Write the header
echo "($ENV_NAME)method,PIEs,MPPs,events,wind_size,rates,avg_process_latency(ns),result,processed_time(ms)" >> $OUT_FILE

# Loop over the parameters and call the Java program
for col in "${cols[@]}"
do
    for limit in "${limits[@]}"
    do
        for wind in "${windSize[@]}"
        do
            for r in "${rates[@]}"
            do
                for ((i = 1; i <= loopNum; i++))
                do

                  # Build the Java command to execute
                  dataPath=$DATA_DIR"events_col"$col"_row10000000.csv"
                  EXEC="$JAVA_CMD  org.piestream.evaluation.Lowlatency $col $limit $wind $dataPath $r "
                  # Execute the command and write the results to the file
                  $EXEC >> $OUT_FILE
                  # Output the result of each execution
                  echo $EXEC
                done
            done
        done
    done
done
