package com.lifetech.hadoop.bioseq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.lifetech.hadoop.streaming.FastaInputFormat;

public class FastaFileFormatApp implements Tool {

	public static class CopyMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class CopyReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}
	}

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileSystem fs = outputPath.getFileSystem(conf);
		
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = new Job(getConf(), "FastaFormatTest");
		job.setJarByClass(FastaFileFormatApp.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setMapperClass(CopyMapper.class);

		job.setReducerClass(CopyReducer.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FastaInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	public static void main(String[] args) {
		Tool app = new FastaFileFormatApp();
		app.setConf(new Configuration());
		try {
			app.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
