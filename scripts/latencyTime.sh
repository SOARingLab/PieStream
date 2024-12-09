#!/bin/bash

# 设置环境变量和输出目录
. ./env

#echo  $JAVA_CMD
OUT_DIR="out/latency_time"

mkdir -p $OUT_DIR

OUT_FILE=$OUT_DIR/latency.txt
echo > $OUT_FILE  # 清空文件

# 定义参数范围
cols=( 4  )
limits=(  1000000 )
windSize=( 10000 )
rates=( 100000 50000 10000 5000 1000 500 )

data_dir="/home/uzi/Code/TPSdata/"
#data_dir="/Users/czq/Code/TPS_data/"


# 循环参数并调用 Java 程序
for col in "${cols[@]}"
do
    for limit in "${limits[@]}"
    do
        for third in "${windSize[@]}"
        do
          for r in "${rates[@]}"
          do
              # 构建执行的 Java 命令
              EXEC="$JAVA_CMD  org.example.evaluation.Lowlatency $col $limit $third $data_dir $r "

              echo "  "  >> $OUT_FILE
              # 输出到文件
              echo   "col=$col,limit=$limit,windSize=$third,rate=$r   " >> $OUT_FILE


              # 执行命令并将结果写入文件
              $EXEC >> $OUT_FILE

              # 输出每次执行的结果
              echo "Executed: $EXEC  "
            done
        done
    done
done

#java  -Xms12g -Xmx12g  -cp "target/classes:lib/*" org.piestream.evaluation.ProcessedTime 4 10000000 100000 /home/uzi/Code/TPSdata/

