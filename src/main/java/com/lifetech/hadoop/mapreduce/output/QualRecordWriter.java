package com.lifetech.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class QualRecordWriter<K> extends RecordWriter<K,BioSeqWritable> {
	private DataOutputStream out;
	
	public QualRecordWriter(DataOutputStream out) {
		this.out = out;
	}
	

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		out.close();
	}

	@Override
	public void write(K key, BioSeqWritable value) throws IOException, InterruptedException {
		if(value == null) return;

		out.writeByte('>');
		out.write(value.getId().getBytes(),0,value.getId().getLength());
		out.writeByte('\n');
		byte[] quals = value.getQuality().getBytes();
		int len = value.getQuality().getLength();
		for(int i = 0;i< len -1;i++) {
			out.writeUTF(Byte.toString(quals[i]));
			out.write(' ');
		}
		out.writeUTF(Byte.toString(quals[len-1]));
		
		out.write(value.getSequence().getBytes(),0,value.getSequence().getLength());
		out.writeByte('\n');
	}

}
