hadoop dfs -rmr hdfs://localhost/user/varuzza/output/
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.CopyFasta hdfs://localhost/user/varuzza/data/test1/input.fasta hdfs://localhost/user/varuzza/output
hadoop dfs -cat hdfs://localhost/user/varuzza/output/part*
