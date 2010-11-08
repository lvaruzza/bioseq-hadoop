package com.lifetech.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.lifetech.hadoop.alignment.AlignWritable;

public class BAMInputFormat extends FileInputFormat<LongWritable,AlignWritable>{

	@Override
	public RecordReader<LongWritable, AlignWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
