#!/bin/bash

# Set environment variables and output directory
. ./env

#echo  $JAVA_CMD
OUT_DIR="out/processed_time"
mkdir -p $OUT_DIR

TIMESTAMP=$(date +"%m%d%H%M")  # Get the current month, day, hour, and minute
OUT_FILE="$OUT_DIR/processTime_$TIMESTAMP.out"  # The filename contains a timestamp

#OUT_FILE=$OUT_DIR/full.txt
echo -n > $OUT_FILE  # Clear the file

# Define the parameter range
cols=( 4 6 8 10 12 14 16 18 20 22 24)
#cols=( 4 6 8 10 )
limits=( 1000  10000   100000  1000000   10000000 )
windList=( 10000 )
loopNum=10

# Write the header
echo "($ENV_NAME)method,PIEs,MPPs,events,wind_size,result,processed_time(ms)" >> $OUT_FILE

# Loop over the parameters and call the Java program
for col in "${cols[@]}"
do
    for limit in "${limits[@]}"
    do
        for wind_size in "${windList[@]}"
        do
          # Loop to repeat execution loopNum times
            for ((i = 1; i <= loopNum; i++))
            do
                # Build the Java command to execute
                dataPath=$DATA_DIR"events_col"$col"_row10000000.csv"
                EXEC="$JAVA_CMD  org.piestream.evaluation.ProcessedTime $col $limit $wind_size $dataPath"
                # Execute the command and write the results to the file
                $EXEC >> $OUT_FILE
                # Output the result of each execution
                echo $EXEC
            done
        done
    done
done
