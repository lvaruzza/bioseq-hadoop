INPUT=hdfs://localhost/home/varuzza/spectrum/

hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.KmerToHbase $INPUT
#echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.DumpKmers $OUTPUT/part-r-00000


