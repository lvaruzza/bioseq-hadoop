package com.lifetech.hadoop.bioseq.converters;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.output.FastaOutputFormat;
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
		return createJob(new Path(outputFileName),true);
	}
	
	protected Job createJob(Path outputPath,boolean writeSequence) throws Exception {
		Path inputPath = new Path(inputFileName);

		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());

			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
		}

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

		job.setOutputFormatClass(FastaOutputFormat.class);
		FastaOutputFormat.setOutputPath(job, outputPath);	
		
		if (writeSequence) {
			FastaOutputFormat.writeSequence();
		} else {
			FastaOutputFormat.writeQuality();			
		}
		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		parseCmdLine(args);
		
		Path outputPath = new Path(outputFileName);

		Job jobFasta = createJob(outputPath,true);
		
		boolean ret1 = jobFasta.waitForCompletion(true);

		Path qualPath = PathUtils.changePathExtension(outputPath, ".qual");
		log.info("Qual file name = " + qualPath);

		Job jobQual = createJob(qualPath,false);

		boolean ret2 = jobQual.waitForCompletion(true);

		return ret1 && ret2 ? 0 : 1;
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
