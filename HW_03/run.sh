set -x
HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
HDFS_OUTPUT_DIR=$2

chmod a+x mapper.py combiner.py reducer.py reducer2.py

hdfs dfs -rm -r -skipTrash ${HDFS_OUTPUT_DIR}*

( yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="Streaming WordCount. Phase 1" \
    -file mapper.py \
    -file combiner.py \
    -file reducer.py \
    -mapper "./mapper.py" \
    -combiner "./combiner.py" \
    -reducer "./reducer.py" \
    -numReduceTasks 8 \
    -input $1 \
    -output ${HDFS_OUTPUT_DIR}_tmp &&

yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="Streaming WordCount. Phase 2" \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2nr" \
    -file reducer2.py \
    -mapper cat \
    -reducer "./reducer2.py" \
    -numReduceTasks 1 \
    -input ${HDFS_OUTPUT_DIR}_tmp \
    -output ${HDFS_OUTPUT_DIR}
) || echo "Error happens"

hdfs dfs -text ${HDFS_OUTPUT_DIR}/*