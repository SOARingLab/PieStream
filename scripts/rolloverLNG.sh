#!/bin/bash

# Set environment variables and output directory
. ./env

#echo  $JAVA_CMD
OUT_DIR="out/lng_rollover"
mkdir -p $OUT_DIR

TIMESTAMP=$(date +"%m%d%H%M")  # Get the current month, day, hour, and minute
OUT_FILE="$OUT_DIR/rollover_$TIMESTAMP.csv"  # The filename contains a timestamp

#OUT_FILE=$OUT_DIR/full.txt
echo -n > $OUT_FILE  # Clear the file

echo "($ENV_NAME)method,PIEs,MPPs,events,wind_size,result,all_used_time(ms),usePreciseRel" >> $OUT_FILE

# Build the Java command to execute
dataPath=$DATA_DIR"LNG_FSRU_simulation_data.csv"


windList=( 10000 50000  100000 200000 400000 600000 800000 1000000  )
usePreciseRelList=(1 0)
limit=8640000
loopNum=10

for usePreciseRel in "${usePreciseRelList[@]}"
do
    for wind in "${windList[@]}"
    do
        for ((i = 1; i <= loopNum; i++))
        do
            EXEC="$JAVA_CMD  org.piestream.evaluation.LNGRollover  $limit $wind $dataPath $usePreciseRel"
            # Execute the command and write the results to the file
            $EXEC >> $OUT_FILE
            # Output the result of each execution
            echo $EXEC
        done
    done
done

