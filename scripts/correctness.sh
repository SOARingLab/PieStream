#!/bin/bash

# Set environment variables and output directory
. ./env

#echo  $JAVA_CMD
OUT_DIR="out/correctness"
mkdir -p $OUT_DIR

TIMESTAMP=$(date +"%m%d%H%M")  # Get the current month, day, hour, and minute
OUT_FILE="$OUT_DIR/correctness_$TIMESTAMP.csv"  # The filename contains a timestamp

#OUT_FILE=$OUT_DIR/full.txt
echo -n > $OUT_FILE  # Clear the file

# Define the parameter range
cols=( 4 )
limits=( 1000 5000  10000 50000 100000 500000 )
include_finish_rels=( 1 0 )
# windSize is equals to limit

# Write the header
echo "($ENV_NAME)method,PIEs,MPPs,events,wind_size,result,all_used_time(ms),if_query_include_finish_rels" >> $OUT_FILE

# Loop over the parameters and call the Java program
for col in "${cols[@]}"
do
    for limit in "${limits[@]}"
    do
            for existFinRel in "${include_finish_rels[@]}"
            do
                # Build the Java command to execute
                dataPath=$DATA_DIR"events_col"$col"_row10000000.csv"
                EXEC="$JAVA_CMD  org.piestream.evaluation.Correctness $col $limit $dataPath $existFinRel"
                # Execute the command and write the results to the file
                $EXEC >> $OUT_FILE
                # Output the result of each execution
                echo $EXEC
            done

    done
done
