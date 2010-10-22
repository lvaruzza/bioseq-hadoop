package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;

/*
 * Import to Hbase kmers to error correction
 */
public class GroupKmers extends Configured implements Tool {

	public static class CopyMapper extends
			Mapper<BytesWritable, IntWritable, BytesWritable, IntWritable> {

		public void map(BytesWritable key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class SumReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int total = 0;

			for (IntWritable count : values) {
				total += count.get();
			}
			context.write(key, new IntWritable(total));
		}
	}

	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Job job = new Job(getConf(), "spectrumBuild");

		job.setJarByClass(KmerHistogram.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(CopyMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,CompressionCodec.class);

		job.setCombinerClass(SumReducer.class);
		
		job.setReducerClass(SumReducer.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new GroupKmers(), args);
		System.exit(ret);
	}
}
