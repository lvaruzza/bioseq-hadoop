hadoop dfs -rmr hdfs://localhost/user/varuzza/output_fastq/
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.FastaToFastq hdfs://localhost/user/varuzza/data/fastaqual/F3.csfasta hdfs://localhost/user/varuzza/data/fastaqual/F3.qual hdfs://localhost/user/varuzza/output_fastq
#rm part*
hadoop dfs -copyToLocal hdfs://localhost/user/varuzza/output_fastq/part* .

