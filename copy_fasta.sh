HADOOP_HOME=/usr/lib/hadoop-0.20
input=$1
output=$2

hadoop dfs -rmr $output

hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*-streaming.jar \
    -libjars target/bioseq-0.0.1.jar \
    -inputformat com.lifetech.hadoop.streaming.FastaInputFormat \
    -input $input  -output $output \
    -mapper "cat" \
    -reducer "cat"
