package com.lifetech.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class FastaQualRecordWriter<K> extends RecordWriter<K,BioSeqWritable> {
	private DataOutputStream fasta;
	private DataOutputStream qual;
	
	public FastaQualRecordWriter(DataOutputStream fasta,DataOutputStream qual) {
		this.fasta = fasta;
		this.qual = qual;
	}
	

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		fasta.close();
		qual.close();
	}

	@Override
	public void write(K key, BioSeqWritable value) throws IOException, InterruptedException {
		if(value == null) return;

		//  Write Fasta
		fasta.writeByte('>');
		fasta.write(value.getId().getBytes(),0,value.getId().getLength());
		fasta.writeByte('\n');
		fasta.write(value.getSequence().getBytes(),0,value.getSequence().getLength());
		fasta.writeByte('\n');
		
		//  Write Qual
		qual.writeByte('>');
		qual.write(value.getId().getBytes(),0,value.getId().getLength());
		qual.writeByte('\n');
		byte[] quals = value.getQuality().getBytes();
		int len = value.getQuality().getLength();
		for(int i = 0;i< len -1;i++) {
			qual.writeBytes(String.format("%d ",quals[i]));
		}
		qual.writeBytes(String.format("%d\n",quals[len-1]));
		
	}

}
