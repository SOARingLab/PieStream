#!/bin/bash

# 设置环境变量和输出目录
. ./env

OUT_DIR="out/latency_time"
mkdir -p $OUT_DIR

TIMESTAMP=$(date +"%m%d%H%M")  # 获取当前的月日时分
OUT_FILE="$OUT_DIR/latency_$TIMESTAMP.out"  # 文件名中包含时间戳

echo > $OUT_FILE  # 清空文件

# 定义参数范围
cols=( 4  )
limits=(  1000000 )
windSize=( 10000 )
#rates=( 100000 50000 10000 5000 1000 500 )

rates=( 100000 50000   )


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
              EXEC="$JAVA_CMD  org.example.evaluation.Lowlatency $col $limit $third $DATA_DIR $r "

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

