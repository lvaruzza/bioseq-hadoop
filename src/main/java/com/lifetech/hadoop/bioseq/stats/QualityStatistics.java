package com.lifetech.hadoop.bioseq.stats;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class QualityStatistics extends Configured implements Tool {

	private String qualFile;
	private String outputFile;

	public static class QualityMapper extends
			Mapper<LongWritable, BioSeqWritable, ByteWritable, ByteWritable> {

		ByteWritable pos = new ByteWritable();
		ByteWritable val = new ByteWritable();

		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			byte[] qual = value.getQuality().getBytes();

			for (byte i = 0; i < value.getQuality().getLength(); i++) {
				pos.set(i);
				val.set(qual[i]);
				context.write(pos, val);
			}
		}
	}

	public static class StatisticsCombiner
			extends
			Reducer<ByteWritable, ByteWritable, ByteWritable, ValueStatsWritable> {

		public void reduce(ByteWritable key, Iterable<ByteWritable> values,
				Context context) throws IOException, InterruptedException {

			ValueStatsWritable result = new ValueStatsWritable();

			for (ByteWritable val : values) {
				result.update(val.get());
			}
			context.write(key, result);
		}
	}

	public static class StatisticsReducer
			extends
			Reducer<ByteWritable, ValueStatsWritable, ByteWritable, ValueStatsWritable> {

		public void reduce(ByteWritable key, Iterable<ValueStatsWritable> values,
				Context context) throws IOException, InterruptedException {

			ValueStatsWritable result = new ValueStatsWritable();

			for (ValueStatsWritable val : values) {
				result.update(val);
			}
			context.write(key, result);
		}
	}

	private void parseCmdLine(String[] args) {
		qualFile = args[0];
		outputFile = args[1];

	}

	@Override
	public int run(String[] args) throws Exception {
		parseCmdLine(args);
		Path qualPath = new Path(qualFile);
		Path outputPath = new Path(outputFile);

		Job job = new Job(getConf(), "qualityStatistics");

		job.setInputFormatClass(FastaInputFormat.class);

		job.setJarByClass(QualityStatistics.class);
		job.setMapperClass(QualityMapper.class);
		job.setCombinerClass(StatisticsCombiner.class);

		job.setReducerClass(StatisticsReducer.class);

		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(ByteWritable.class);

		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(ValueStatsWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FastaInputFormat.setInputPaths(job, qualPath);
		FastqOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new QualityStatistics(), args);
	}

}