package com.lifetech.hadoop.bioseq;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FastaFileFormat extends FileInputFormat<LongWritable, Text> {

	 private CompressionCodecFactory compressionCodecs = null;
	 
	 @Override
	  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
	                       TaskAttemptContext context) {
		 
	    return new FastaRecordReader();
	  }

	  @Override
	  protected boolean isSplitable(JobContext context, Path file) {
		  return compressionCodecs.getCodec(file) == null;		  
	  }
}
