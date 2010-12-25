package com.lifetech.hadoop.bioseq.stats;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.bioseq.FourBitsEncoder;

public class SequenceDups extends CLIApplication implements Tool {
	private static Logger log = Logger.getLogger(SequenceDups.class);
	private static BioSeqEncoder encoder = new FourBitsEncoder();

	public static class CountSeqMapper extends
			Mapper<Text, BioSeqWritable, BytesWritable, IntWritable> {

		// private BytesWritable kmer = new BytesWritable();
		private IntWritable ONE = new IntWritable(1);

		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			Text seq = value.getSequence();
			context.write(encoder.encode(seq), ONE);
		}
	}

	public static class SumCombiner extends
			Reducer<BytesWritable, IntWritable, BytesWritable, IntWritable> {

		public void reduce(BytesWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable count : values) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class SumReducer extends
			Reducer<BytesWritable, IntWritable, Text, IntWritable> {

		public void reduce(BytesWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable count : values) {
				sum += count.get();
			}
			if (sum > 1) {
				context.write(encoder.decode(key), new IntWritable(sum));
			}
		}
	}

	protected Options buildOptions() {
		// create Options object
		Options options = new Options();

		this.addInputOptions(options);
		addOutputOptions(options);

		return options;
	}

	protected void checkCmdLine(Options options, CommandLine cmd) {
		this.checkInputOptionsInCmdLine(options, cmd);
		this.checkOutputOptionsInCmdLine(options, cmd);
	}


	@Override
	protected Job createJob() throws Exception {
		Path inputPath = new Path(inputFileName);
		Path outputPath = new Path(outputFileName);

		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());
			log.info(String.format("Removing '%s'", outputFileName));
			fs.delete(outputPath, true);
		}

		Job job = new Job(getConf(), appName());

		job.setJarByClass(SequenceDups.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(CountSeqMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(SumCombiner.class);

		job.setReducerClass(SumReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextOutputFormat.setOutputPath(job, outputPath);
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SequenceDups(), args);
		System.exit(ret);
	}

	@Override
	protected String appName() {
		return "sequenceDups";
	}
}
