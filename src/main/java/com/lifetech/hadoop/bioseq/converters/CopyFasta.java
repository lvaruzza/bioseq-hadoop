package com.lifetech.hadoop.bioseq.converters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastaOutputFormat;

public class CopyFasta implements Tool {

	public static class CopyMapper extends
			Mapper<LongWritable, BioSeqWritable, LongWritable, BioSeqWritable> {

		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class CopyReducer extends
			Reducer<LongWritable, BioSeqWritable, LongWritable, BioSeqWritable> {

		public void reduce(LongWritable key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {
			for (BioSeqWritable val : values) {
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
		
		/*if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}*/

		Job job = new Job(getConf(), "CopyFasta");
		job.setJarByClass(CopyFasta.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setMapperClass(CopyMapper.class);

		job.setReducerClass(CopyReducer.class);
		
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(LongWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(FastaOutputFormat.class);
		
		FastaInputFormat.setInputPaths(job, inputPath);
		FastaOutputFormat.setOutputPath(job, outputPath);
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
		Tool app = new CopyFasta();
		app.setConf(new Configuration());
		try {
			app.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
