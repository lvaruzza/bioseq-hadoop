package com.lifetech.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class FastqRecordWriter<K> extends RecordWriter<K,BioSeqWritable> {
	private DataOutputStream out;
	
	public FastqRecordWriter(DataOutputStream out) {
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

		out.writeByte('@');
		out.write(value.getId().getBytes());
		out.writeByte('\n');
		out.write(value.getSequence().getBytes());
		out.writeBytes("+\n");
		out.write(value.getQuality().getBytes());
		out.writeByte('\n');
	}

}