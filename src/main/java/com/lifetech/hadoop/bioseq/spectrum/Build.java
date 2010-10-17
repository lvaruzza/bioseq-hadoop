package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class Build extends Configured implements Tool {

	public static class KmerBuilder extends
			Mapper<Text, BioSeqWritable, Text, KmerTracking> {

		private Text kmer = new Text();
		private KmerTracking tracking = new KmerTracking();

		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			int k = context.getConfiguration().getInt("spectrum.k", 15);

			Text seq = value.getSequence();
			int size = seq.getLength();
			byte[] data = seq.getBytes();
			for (int i = 0; i < size - k; i++) {
				kmer.set(data, i, k);
				tracking.set(value.getId(), i);
				context.write(kmer, tracking);
			}
		}
	}

	public static class MergeReducer extends
			Reducer<Text, KmerTracking, Text, KmerTracking> {

		public void reduce(Text key, Iterable<KmerTracking> values,
				Context context) throws IOException, InterruptedException {
			KmerTracking value = new KmerTracking();

			for (KmerTracking kmer : values) {
				value.addAll(kmer);
			}
			context.write(key, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Job job = new Job(getConf(), "spectrumBuild");

		job.setJarByClass(Build.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(KmerBuilder.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KmerTracking.class);

		job.setReducerClass(MergeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KmerTracking.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Build(), args);
		System.exit(ret);
	}
}
