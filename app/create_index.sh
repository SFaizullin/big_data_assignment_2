#!/bin/bash
INPUT_DATA=${1:-"/input/data"}

hdfs dfs -rm -r -f /indexer/tmp1
hdfs dfs -rm -r -f /indexer/index

# Pipeline 1
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -file "mapreduce/mapper1.py" -mapper "python3 mapper1.py" \
    -file "mapreduce/reducer1.py" -reducer "python3 reducer1.py" \
    -input $INPUT_DATA -output /indexer/tmp1

# Pipeline 2
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -file "mapreduce/mapper2.py" -mapper "python3 mapper2.py" \
    -file "mapreduce/reducer2.py" -reducer "python3 reducer2.py" \
    -input /indexer/tmp1 -output /indexer/index