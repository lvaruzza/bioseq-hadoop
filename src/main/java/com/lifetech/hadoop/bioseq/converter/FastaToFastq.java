package com.lifetech.hadoop.bioseq.converter;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
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

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class FastaToFastq extends Configured implements Tool {
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
			
			context.write(empty, new BioSeqWritable(
					key,
					seq == null ? null : seq,
					qual == null ? null : qual));
		}
	}

	private String fastaFile;
	private String qualFile;
	private String fastqFile;
	private boolean addFirstQualValue;
	
	private void exit(int exitStatus) {
		if (exitStatus == 0 ){
			log.info("Program successfully finishied");
		} else {
			log.info("Program finishied with ERROR!!!!");			
		}
		System.exit(exitStatus);
	}
	private void help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "FastaToFastq", options );
	}
	
	private void parseCmdLine(String[] args) throws ParseException {
		// create Options object
		Options options = new Options();

		// add t option
		options.addOption("bfast", false, "Generete fastq file compatible with bfast's solid2fastq program");
		options.addOption("f","fasta", true, "Fasta/csfasta input file");
		options.addOption("q","qual", true, "Qual file");
		options.addOption("o","output", true, "Output fastq file");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);
		
		if (cmd.hasOption("f")) {
			fastaFile = cmd.getOptionValue("f");
			log.info(String.format("Input fasta file '%s'", fastaFile));
		} else {
			log.error(String.format("Missing mandatory argument -f / --fasta"));			
			help(options);
			exit(-1);
		}


		if (cmd.hasOption("q")) {
			qualFile = cmd.getOptionValue("q");
			log.info(String.format("Input Qual file '%s'", qualFile));
		} else {
			log.error(String.format("Missing mandatory argument -q / --qual"));			
			help(options);
			exit(-1);
		}

		
		if (cmd.hasOption("o")) {
			fastqFile = cmd.getOptionValue("o");
			log.info(String.format("Output fastQ file '%s'", fastqFile));
		} else {
			log.error(String.format("Missing mandatory argument -o / --output"));			
			help(options);
			exit(-1);
		}
		if (cmd.hasOption("bfast")) {
			addFirstQualValue = true;
			log.info("Make fastq compatible with bfast's solid2fastq");
		} else {
			addFirstQualValue = false;			
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		parseCmdLine(args);
		Path fastaPath = new Path(fastaFile);
		Path qualPath = new Path(qualFile);
		Path outputPath = new Path(fastqFile);
		
		//FileSystem fs = outputPath.getFileSystem(conf);		
		/*if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}*/

		
		Job job = new Job(getConf(), "FastaToSequenceFile");
		
		getConf().setBoolean("fastaformat.addFistQualityValue", addFirstQualValue);
		
		
		if (fastaPath.getName().endsWith(".csfasta")) {
			System.out.println("Color Space Fasta");
			job.getConfiguration().set("bioseq.colorSpaceInput", "true");
		}
				
		
		job.setJarByClass(FastaToFastq.class);
		job.setInputFormatClass(FastaInputFormat.class);
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
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new FastaToFastq(), args);
		System.exit(ret);
	}
}
