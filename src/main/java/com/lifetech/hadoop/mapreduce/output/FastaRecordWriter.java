package com.lifetech.hadoop.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FastaRecordWriter<K,V> extends RecordWriter<K,V> {

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

}
