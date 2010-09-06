package com.lifetech.hadoop.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FastqOutputFormat<K> extends FileOutputFormat<K,Text> {

	@Override
	public RecordWriter<K, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
