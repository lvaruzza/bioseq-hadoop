hadoop dfs -rmr hdfs://localhost/user/varuzza/output_seqfile/
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.FastaToSequenceFile hdfs://localhost/user/varuzza/data/fastaqual/F3.csfasta hdfs://localhost/user/varuzza/data/fastaqual/F3.qual hdfs://localhost/user/varuzza/output_seqfile
#rm part*
#hadoop dfs -copyToLocal hdfs://localhost/user/varuzza/output_seqfile/part* .
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.DumpSeqFile hdfs://localhost/user/varuzza/output_seqfile/part-r-00000
