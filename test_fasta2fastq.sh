OUTPUT=hdfs://localhost/user/varuzza/output_fastq2/
hadoop dfs -rmr $OUTPUT
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.FastaToFastq hdfs://localhost/user/varuzza/data/fastaqual/F3.csfasta hdfs://localhost/user/varuzza/data/fastaqual/F3.qual $OUTPUT
rm part*
hadoop dfs -copyToLocal $OUTPUT/part* .
