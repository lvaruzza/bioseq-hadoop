package com.lifetech.hadoop.bioseq.transform;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoCodec;
import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class SequenceSampler extends CLIApplication {
	private static Logger log = Logger.getLogger(SequenceSampler.class);

	private float percent;

	
	public static class SamplerMapper extends
			Mapper<Text, BioSeqWritable, Text, BioSeqWritable> {

		private  Random generator = new Random();
		
		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			float p = context.getConfiguration().getFloat("sequenceSampler.percent", (float) 0.1);

			float x = generator.nextFloat();
			if (x<=p) {
				context.write(key, value);
			}
		}
	}
	
	public static class CopyReducer extends
			Reducer<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void reduce(Text key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {

			for (BioSeqWritable val : values) {
				context.write(key, val);
			}
		}
	}

	@Override
	protected Options buildOptions() {
		Options options = new Options();
		addOutputOptions(options);
		addInputOptions(options);

		options.addOption("p", "percentAccept", true,
				"percentage of reads to be accespted");
		return options;
	}

	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		this.checkOutputOptionsInCmdLine(options, cmd);
		this.checkInputOptionsInCmdLine(options, cmd);

		if (cmd.hasOption("p")) {
			percent = Float.parseFloat(cmd.getOptionValue("p"));
		} else {
			log.error(String
					.format("Missing mandatory argument -p / --percentAccept"));
			help(options);
			exit(-1);
		}
	}

	@Override
	protected Job createJob() throws Exception {
		Path inputPath = new Path(inputFileName);
		Path outputPath = new Path(outputFileName);

		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());
			if (fs.exists(outputPath)) {
				log.info(String.format("Removing '%s'", outputPath));
				fs.delete(outputPath, true);
			}
		}

		getConf().setFloat("sequenceSampler.percent", percent);

		Job job = new Job(getConf(), "FastaToSequenceFile");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		
		job.setJarByClass(SequenceSampler.class);
		job.setMapperClass(SamplerMapper.class);

		job.setReducerClass(CopyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BioSeqWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		return job;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SequenceSampler(), args);
		System.exit(ret);
	}
}
