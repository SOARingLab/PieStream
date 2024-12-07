#!/bin/bash

# 设置环境变量和输出目录
. ./env

#echo  $JAVA_CMD
OUT_DIR="out/processed_time" 

mkdir -p $OUT_DIR

OUT_FILE=$OUT_DIR/full.txt
echo > $OUT_FILE  # 清空文件

# 定义参数范围
#cols=( 4 6 8 10 12 14 16 18 20 22 24 26 28 30 )

cols=(  6 22 24  )
# 4 6 8 10
#limits=( 1000 5000 10000 50000 100000 500000 1000000 5000000 10000000 )

limits=( 1000   10000   100000   1000000   10000000 )
windSize=(    10000 )
data_dir="/home/uzi/Code/TPSdata/"

# 写入表头
echo -n "col,limit,windSize" >> $OUT_FILE
echo ",output" >> $OUT_FILE

# 循环参数并调用 Java 程序
for col in "${cols[@]}"
do
    for limit in "${limits[@]}"
    do
        for third in "${windSize[@]}"
        do
            # 构建执行的 Java 命令
            EXEC="$JAVA_CMD  org.example.evaluation.ProcessedTime $col $limit $third $data_dir"
            
            # 输出到文件
            echo -n "$col,$limit,$third" >> $OUT_FILE
            echo -n "," >> $OUT_FILE
            
            # 执行命令并将结果写入文件
            $EXEC >> $OUT_FILE
            
            # 输出每次执行的结果
            echo "Executed: $EXEC \n"
        done
    done
done

#java  -Xms12g -Xmx12g  -cp "target/classes:lib/*" org.example.evaluation.ProcessedTime 4 10000000 100000 /home/uzi/Code/TPSdata/

