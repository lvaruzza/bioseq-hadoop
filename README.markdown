To run using hadoop-streaming:

    hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2+320.jar \
    -libjars bioseq-0.0.1.jar -inputformat com.lifetech.hadoop.streaming.FastaInputFormat \
    -input marafo/jaguara.fasta -output out12 -mapper "cat" -jobconf start.token=">"

To run the java mapreduce test (Old API)

    mvn package && hadoop jar target/bioseq-0.0.1.jar \
      com.lifetech.hadoop.bioseq.FastaFileFormatOldAPIApp \
      file://$PWD/data/test1/input.fasta data/output/ \


To run the java mapreduce test (New API)

    mvn package && hadoop jar target/bioseq-0.0.1.jar \
      com.lifetech.hadoop.bioseq.FastaFileFormatApp \
      file://$PWD/data/test1/input.fasta data/output/ \

