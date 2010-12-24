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
import org.uncommons.maths.statistics.DataSet;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class QualityMedianStatistics extends Configured implements Tool {

	private String qualFile;
	private String outputFile;

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
		FastaInputFormat.setInputPaths(job, qualPath);

		job.setJarByClass(QualityMedianStatistics.class);

		job.setMapperClass(QualityMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(ValueStatsWritable.class);
		
		job.setCombinerClass(StatisticsReducer.class);
		job.setReducerClass(StatisticsReducer.class);

		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(ValueStatsWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FastqOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new QualityMedianStatistics(), args);
	}

}
