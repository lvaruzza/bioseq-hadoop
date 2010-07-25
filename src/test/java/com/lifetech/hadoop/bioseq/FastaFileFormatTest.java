package com.lifetech.hadoop.bioseq;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FastaFileFormatTest {

	  public static class CountMapper 
	       extends Mapper<Object, Text, Text, IntWritable>{
	    
	    private final static IntWritable one = new IntWritable(1);
	    private final static Text total = new Text("total");
	    
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	        context.write(total, one);
	    }
	  }
	  
	  public static class IntSumReducer 
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
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
	    job.setMapperClass(CountMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("tests/test1/input.fasta"));
	    FileOutputFormat.setOutputPath(job,outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
