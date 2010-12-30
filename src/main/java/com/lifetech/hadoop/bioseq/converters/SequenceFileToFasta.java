package com.lifetech.hadoop.bioseq.converters;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.utils.PathUtils;

public class SequenceFileToFasta extends CLIApplication {
	private static Logger log = Logger.getLogger(SequenceFileToFasta.class);

	public static class CopyMapper extends
			Mapper<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class SplitReducer extends
			Reducer<Text, BioSeqWritable, Text, BioSeqWritable> {

		private MultipleOutputs multipleOutputs;

		@Override
		public void configure(Job conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}

		public void reduce(Text key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {
		}
	}

	@Override
	protected Options buildOptions() {
		Options options = new Options();

		this.addInputOptions(options);
		this.addOutputOptions(options);

		return options;
	}

	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		this.checkInputOptionsInCmdLine(options, cmd);
		this.checkOutputOptionsInCmdLine(options, cmd);
	}

	@Override
	protected Job createJob() throws Exception {
		Path inputPath = new Path(inputFileName);
		Path outputPath = new Path(outputFileName);
		Path qualPath = PathUtils.changePathExtension(outputPath, ".qual");

		log.info("Qual file name = " + qualPath);

		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());

			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			if (fs.exists(qualPath)) {
				fs.delete(qualPath, true);
			}
		}

		Job job = new Job(getConf(), appName());

		job.setJarByClass(SequenceFileToFasta.class);

		job.setMapperClass(CopyMapper.class);
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);

		return null;
	}

	@Override
	protected String appName() {
		return "sequenceToFasta";
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SequenceFileToFasta(), args);
		System.exit(ret);
	}
}
