OUTPUT=hdfs://localhost/user/varuzza/output_seqfile2/
echo hadoop dfs -rmr $OUTPUT
echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.FastaToSequenceFile hdfs://localhost/user/varuzza/data/fastaqual/F3.csfasta hdfs://localhost/user/varuzza/data/fastaqual/F3.qual $OUTPUT
#rm part*
#hadoop dfs -copyToLocal hdfs://localhost/user/varuzza/output_seqfile/part* .
echo hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.DumpSeqFile $OUTPUT/part-r-00000
