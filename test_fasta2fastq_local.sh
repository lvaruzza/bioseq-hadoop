#hadoop dfs -rmr hdfs://localhost/user/varuzza/output_fastq/
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.FastaToFastq data/fastaqual/F3.csfasta data/fastaqual/F3.qual output_fastq

