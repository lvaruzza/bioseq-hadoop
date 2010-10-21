
FASTA=file:///data/rosalind/Rosalind_20080729_2_Chris5_F3.csfasta
QUAL=file:///data/rosalind/Rosalind_20080729_2_Chris5_F3_QV.qual
SEQFILE=/user/varuzza/ecoli/input
SPECTRUM=hdfs://localhost/user/varuzza/ecoli/spectrum/build

# Make seqfile
echo Converting from csfasta/qual to sequenceFile
hadoop dfs -rmr $SEQFILE
time hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.demos.FastaToSequenceFile $FASTA $QUAL $SEQFILE

# make spectrum
echo Building spectrum
hadoop dfs -rmr $SPECTRUM
time hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.Build -libjars target/lib/commons-lang-*.jar $SEQFILE $SPECTRUM


# Dump spectrum
hadoop jar target/bioseq-0.0.1.jar com.lifetech.hadoop.bioseq.spectrum.DumpKmers $OUTPUT/part-r-00000
