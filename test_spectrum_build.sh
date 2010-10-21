INPUT=hdfs://localhost/user/varuzza/spectrum/input/part-r-00000
OUTPUT=hdfs://localhost/user/varuzza/spectrum/build

echo hadoop dfs -rmr $OUTPUT
echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.Build -libjars target/lib/commons-lang-*.jar $INPUT $OUTPUT
echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.DumpKmers $OUTPUT/part-r-00000


