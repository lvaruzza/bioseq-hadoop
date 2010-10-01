package com.lifetech.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.utils.FastqUtils;

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

	private void writeText(Text text) throws IOException {
		out.write(text.getBytes(),0,text.getLength());		
	}
	
	@Override
	public void write(K key, BioSeqWritable value) throws IOException, InterruptedException {
		if(value == null) return;

		out.writeByte('@');
		writeText(value.getId());		
		out.writeBytes("\n");
		writeText(value.getSequence());
		out.writeByte('\n');
		out.writeBytes("+\n");
		writeText(FastqUtils.fastqQuality(value.getQuality()));
		out.writeByte('\n');
	}

}
