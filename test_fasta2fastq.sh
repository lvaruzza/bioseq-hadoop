OUTPUT=hdfs://localhost/user/varuzza/output_fastq/
FASTA=hdfs://localhost/user/varuzza/data/fastaqual/F3.csfasta
QUAL=hdfs://localhost/user/varuzza/data/fastaqual/F3.qual

hadoop dfs -rmr $OUTPUT
./bin/fasta2fastq -f $FASTA -q $QUAL -o $OUTPUT
hadoop dfs -cat $OUTPUT/part* > F3.fastq
