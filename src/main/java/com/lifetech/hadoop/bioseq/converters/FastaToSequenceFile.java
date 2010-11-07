package com.lifetech.hadoop.bioseq.converters;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoCodec;
import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.input.FastaRecordReader;

public class FastaToSequenceFile extends CLIApplication implements Tool {
    private static Logger log = Logger.getLogger(FastaToSequenceFile.class);

	public static class CopyMapperWithId extends
			Mapper<LongWritable, BioSeqWritable, Text, BioSeqWritable> {

		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(value.getId(), value);
		}
	}
	
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
			
			context.write(key, new BioSeqWritable(key,seq,qual));
		}
	}

	private String fastaFileName;
	private String qualFileName;
	
	protected Options buildOptions() {
		// create Options object
		Options options = new Options();

		// add t option
		options.addOption("f","fasta", true, "Fasta/csfasta input file");
		options.addOption("q","qual", true, "Qual file");

		addOutputOptions(options);
		
		return options;
	}

	protected void checkCmdLine(Options options,CommandLine cmd) {		
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
		this.checkOutputOptionsInCmdLine(options, cmd);
	}

	@Override
	protected Job createJob() throws Exception {
		Path fastaPath = new Path(fastaFileName);
		Path qualPath = new Path(qualFileName);
		Path outputPath = new Path(outputFileName);
		
		
		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());		
			if (fs.exists(outputPath)) {
				log.info(String.format("Removing '%s'", outputPath));
				fs.delete(outputPath, true);
			}
		}
		
		if (fastaPath.getName().endsWith(".csfasta")) {
			log.info("Color Space Fasta");
			getConf().setBoolean("fastaformat.addFistQualityValue", true);
			log.info("fastaformat.addFistQualityValue set to true");
		}
		
		Job job = new Job(getConf(), "FastaToFastq");
						
		job.setJarByClass(FastaToSequenceFile.class);
		job.setInputFormatClass(FastaInputFormat.class);
		FastaInputFormat.setInputPaths(job, fastaPath,qualPath);

		job.setMapperClass(CopyMapperWithId.class);
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,CompressionCodec.class);
		getConf().setStrings("mapred.output.compression.type", "BLOCK");
		job.setReducerClass(MergeReducer.class);			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		return job;
	}

	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new FastaToSequenceFile(), args);
		System.exit(ret);
	}
}
