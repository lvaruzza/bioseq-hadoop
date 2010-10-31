INPUT=hdfs://localhost/home/varuzza/data/seqfile/part-r-00000
OUTPUT=hdfs://localhost/home/varuzza/spectrum/

echo hadoop dfs -rmr $OUTPUT
echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.SpectrumBuilder -libjars target/lib/commons-lang-*.jar $INPUT $OUTPUT
#echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.DumpKmers $OUTPUT/part-r-00000


