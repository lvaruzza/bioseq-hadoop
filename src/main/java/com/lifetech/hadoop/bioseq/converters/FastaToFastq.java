package com.lifetech.hadoop.bioseq.converters;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class FastaToFastq extends CLIApplication implements Tool {
    private static Logger log = Logger.getLogger(FastaToFastq.class);

	public static class CopyMapperWithId extends
			Mapper<LongWritable, BioSeqWritable, Text, BioSeqWritable> {

		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(value.getId(), value);
		}
	}

	private static Text empty = new Text("");
	
	public static class MergeReducer extends
			Reducer<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void reduce(Text key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {
			Text seq = new Text();
			BytesWritable qual = new BytesWritable();
			
			for (BioSeqWritable val : values) {
				if (val.getType() == BioSeqWritable.BioSeqType.SequenceOnly)
					seq.set(val.getSequence());
				else if (val.getType() == BioSeqWritable.BioSeqType.QualityOnly)
					qual.set(val.getQuality());
				else
					throw new RuntimeException(String.format("Invalid SeqType '%s' in sequence '%s'", 
							val.getType().name(),val.getId().toString()));
			}
			
			context.write(empty, new BioSeqWritable(key,seq,qual));
		}
	}

	private String fastaFileName;
	private String qualFileName;
	private boolean addFirstQualValue;
	
	@Override
	protected Options buildOptions() {
		// create Options object
		Options options = new Options();

		// add t option
		options.addOption("bfast", false, "Generete fastq file compatible with bfast's solid2fastq program");
		options.addOption("f","fasta", true, "Fasta/csfasta input file");
		options.addOption("q","qual", true, "Qual file");
		addOutputOptions(options);
		
		return options;
	}
	
	@Override
	protected void checkCmdLine(Options options,CommandLine cmd)  {
		if (cmd.hasOption("f")) {
			fastaFileName = cmd.getOptionValue("f");
			log.info(String.format("Input fasta file '%s'", fastaFileName));
		} else {
			log.error(String.format("Missing mandatory argument -f / --fasta"));			
			help(options);
			exit(-1);
		}


		if (cmd.hasOption("q")) {
			qualFileName = cmd.getOptionValue("q");
			log.info(String.format("Input Qual file '%s'", qualFileName));
		} else {
			log.error(String.format("Missing mandatory argument -q / --qual"));			
			help(options);
			exit(-1);
		}

		
		if (cmd.hasOption("bfast")) {
			addFirstQualValue = false;
			log.info("Make fastq compatible with bfast's solid2fastq");
		} else {
			addFirstQualValue = true;			
		}
		
		checkOutputOptionsInCmdLine(options, cmd);
	}
	
	@Override
	protected Job createJob() throws Exception {
		Path fastaPath = new Path(fastaFileName);
		Path qualPath = new Path(qualFileName);
		Path outputPath = new Path(outputFileName);
		
		getConf().setBoolean(FastaInputFormat.addFistQualityValueProperty , addFirstQualValue);

		
		Job job = new Job(getConf(), appName());
		
		job.setInputFormatClass(FastaInputFormat.class);
		
		job.setJarByClass(FastaToFastq.class);
		job.setMapperClass(CopyMapperWithId.class);

		job.setReducerClass(MergeReducer.class);
		
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(FastqOutputFormat.class);

		FastaInputFormat.setInputPaths(job, fastaPath,qualPath);
		//FastaInputFormat.setInputPaths(job,qualPath);
		FastqOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FastaToFastq(), args);
	}
	
	@Override
	protected String appName() {
		return "FastaToFastq";
	}
}
