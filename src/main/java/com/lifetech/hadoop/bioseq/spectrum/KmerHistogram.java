package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;

public class KmerHistogram extends Configured implements Tool {

	public static class HistogramMapper extends
			Mapper<Text, LongWritable, LongWritable, LongWritable> {

		private LongWritable ONE= new LongWritable(1);

		public void map(Text key, LongWritable value, Context context)
				throws IOException, InterruptedException {

			context.write(value, ONE);
		}
	}

	public static class SumReducer extends
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			long total = 0;
			
			for (LongWritable count : values) {
				total += count.get(); 
			}
			context.write(key, new LongWritable(total));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int ret = doMapReduce(args);		
		return ret;
	}
	
	public int doMapReduce(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Job job = new Job(getConf(), "spectrumBuild");

		job.setJarByClass(KmerHistogram.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(HistogramMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,CompressionCodec.class);

		job.setCombinerClass(SumReducer.class);
		
		job.setReducerClass(SumReducer.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new KmerHistogram(), args);
		System.exit(ret);
	}
}
