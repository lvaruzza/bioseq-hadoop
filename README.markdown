To run using hadoop-streaming:

    hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2+320.jar \
    -libjars bioseq-0.0.1.jar -inputformat com.lifetech.hadoop.streaming.FastaInputFormat \
    -input marafo/jaguara.fasta -output out12 -mapper "cat" -jobconf start.token=">"
