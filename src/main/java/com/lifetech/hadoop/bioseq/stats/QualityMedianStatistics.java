package com.lifetech.hadoop.bioseq.stats;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.uncommons.maths.statistics.DataSet;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class QualityMedianStatistics extends CLIApplication implements Tool {

	public static class QualityMapper extends
			Mapper<LongWritable, BioSeqWritable, ByteWritable, LongWritable> {

		private static LongWritable ONE = new LongWritable(1);
		private ByteWritable medianQual = new ByteWritable();
		
		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			byte[] quals = value.getQuality().getBytes();
			int len = value.getQuality().getLength();
			
			DataSet stats = new DataSet();
			
			for(int i=0;i<len;i++) {
				stats.addValue(quals[i]);
			}
			medianQual.set((byte) stats.getMedian());
			context.write(medianQual, ONE);
		}
	}

	public static class StatisticsReducer
			extends
			Reducer<ByteWritable, LongWritable, ByteWritable, LongWritable> {

		private LongWritable result = new LongWritable();
		
		public void reduce(ByteWritable key,
				Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}


	@Override
	protected Job createJob() throws Exception {
		Path qualPath = new Path(this.inputFileName);
		Path outputPath = new Path(this.outputFileName);

		Job job = new Job(getConf(), appName());

		maybeRemoevOldOutput(outputPath);
		
		job.setInputFormatClass(FastaInputFormat.class);
		FastaInputFormat.setInputPaths(job, qualPath);

		job.setJarByClass(QualityMedianStatistics.class);

		job.setMapperClass(QualityMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setCombinerClass(StatisticsReducer.class);
		job.setReducerClass(StatisticsReducer.class);

		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FastqOutputFormat.setOutputPath(job, outputPath);
		return job;
	}


	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		checkInputOptionsInCmdLine(options, cmd);
		checkOutputOptionsInCmdLine(options, cmd);
	}

	@Override
	protected Options buildOptions() {
		Options options = new Options();
		addInputOptions(options);
		addOutputOptions(options);
		return options;
	}


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new QualityMedianStatistics(), args);
	}


	@Override
	protected String appName() {
		return "qualityMedianStatistics";
	}


}
