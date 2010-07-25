package com.lifetech.hadoop.bioseq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FastaFileFormatTest {

	  public static class CopyMapper 
	       extends Mapper<LongWritable, Text, LongWritable, Text>{
	    
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	        context.write(key, value);
	    }
	  }
	  
	  public static class CopyReducer 
	       extends Reducer<LongWritable,Text,LongWritable,Text> {

	    public void reduce(LongWritable key, Iterable<Text> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	      for (Text val : values) {
		      context.write(key, val);
	      }
	    }
	  }

	  private static Path outputPath  =  new Path("tests/test1/output");
	  
	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    FileSystem fs = outputPath.getFileSystem(conf);
	    
	    if (fs.exists(outputPath)) {
	    	fs.delete(outputPath, true);
	    }
	    		
	    Job job = new Job(conf, "FastaFormatTest");
	    job.setJarByClass(FastaFileFormatTest.class);
	    job.setMapperClass(CopyMapper.class);

	    job.setReducerClass(CopyReducer.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path("tests/test1/input.fasta"));
	    FileOutputFormat.setOutputPath(job,outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
