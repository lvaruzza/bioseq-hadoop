OUTPUT=/home/varuzza/output_fastq/
FASTA=/home/varuzza/data/fastaqual/F3.csfasta
QUAL=/home/varuzza/data/fastaqual/F3.qual

hadoop dfs -rmr $OUTPUT
./bin/fasta2fastq -f $FASTA -q $QUAL -o $OUTPUT
hadoop dfs -cat $OUTPUT/part* > F3.fastq
