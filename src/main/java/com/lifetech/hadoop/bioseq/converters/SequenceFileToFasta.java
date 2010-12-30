package com.lifetech.hadoop.bioseq.converters;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.output.FastaQualOutputFormat;

public class SequenceFileToFasta extends CLIApplication {
	private static Logger log = Logger.getLogger(SequenceFileToFasta.class);

	public static class CopyMapper extends
			Mapper<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
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

	protected Job createJob() throws Exception {
		Path inputPath = new Path(inputFileName);
		Path outputPath = new Path(outputFileName);
		
		this.maybeRemoevOldOutput(outputPath);

		Job job = new Job(getConf(), appName());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		
		job.setJarByClass(SequenceFileToFasta.class);

		job.setMapperClass(CopyMapper.class);
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);

		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(FastaQualOutputFormat.class);
		FastaQualOutputFormat.setOutputPath(job, outputPath);	
		return job;
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
