set -x
HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
chmod a+x mapper.py reducer.py
hdfs dfs -rm -r $2
yarn jar $HADOOP_STREAMING_JAR \
 -file mapper.py \
 -file reducer.py \
 -mapper "./mapper.py" \
 -reducer "./reducer.py" \
 -numReduceTasks 2 \
 -input $1 \
 -output $2
hdfs dfs -text hw2_mr_data_ids/* | head -n 50