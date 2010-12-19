package com.lifetech.hadoop.alignment.converters;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
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
import com.lifetech.hadoop.alignment.GFFParser;
import com.lifetech.hadoop.alignment.GFFRec;
import com.lifetech.hadoop.alignment.GeneWritable;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;

public class GTFToSequenceFile extends CLIApplication implements Tool {
	private static Logger log = Logger.getLogger(GTFToSequenceFile.class);

	public static class CopyMapperWithId extends
			Mapper<LongWritable, Text, Text, GFFRec> {

		private static GFFParser parser = new GFFParser();
		
		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {
			
			GFFRec gff = parser.parse(line);
			
			context.write(new Text(/* something*/), gff);
		}
	}

	public static class MergeReducer extends
			Reducer<Text, GFFRec, Text, GeneWritable> {

		public void reduce(Text key, Iterable<GFFRec> values,
				Context context) throws IOException, InterruptedException {

			//context.write(key, new BioSeqWritable(key, seq, qual));
		}
	}

	public static class ConstantQualReducer extends
			Reducer<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void reduce(Text key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {

			byte qv = (byte) context.getConfiguration().getInt("", 10);
			
			Text seq = values.iterator().next().getSequence();
			byte[] quals = new byte[seq.getLength()];
			Arrays.fill(quals, qv);
			BytesWritable qual = new BytesWritable(quals);
			
			context.write(key, new BioSeqWritable(key, seq , qual));
		}
	}

	private String fastaFileName = null;
	private String qualFileName = null;
	private int constantQualValue;
	private boolean useQualFile;

	protected Options buildOptions() {
		// create Options object
		Options options = new Options();

		// add t option
		options.addOption("f", "fasta", true, "Fasta/csfasta input file");
		options.addOption("q", "qual", true, "Qual file");
		options.addOption("Q", "qual-value", true, "Contant qual value");

		addOutputOptions(options);

		return options;
	}

	protected void checkCmdLine(Options options, CommandLine cmd) {
		if (cmd.hasOption("f")) {
			fastaFileName = cmd.getOptionValue("f");
			log.info(String.format("Input fasta file '%s'", fastaFileName));
		} else {
			log.error(String.format("Missing mandatory argument -f / --fasta"));
			help(options);
			exit(-1);
		}

		if (cmd.hasOption("q") && cmd.hasOption("Q")) {
			log.error(String.format("Can't use -q and -Q options together (this does not make sense)"));
			help(options);
			exit(-1);
		}

		if (cmd.hasOption("q")) {
			qualFileName = cmd.getOptionValue("q");
			log.info(String.format("Input Qual file '%s'", qualFileName));
			useQualFile = true;
		} else {
			useQualFile = false;
			if (cmd.hasOption("Q")) {
				constantQualValue = Integer.parseInt(cmd.getOptionValue("Q"));
				log.info(String.format("Constant Qual Value '%d'", constantQualValue));
			} else {
				constantQualValue = 10;
				log.info(String.format("No qual file specified, using Default Constant Qual Value '%d'",
										constantQualValue));

			}
		}
		this.checkOutputOptionsInCmdLine(options, cmd);
	}

	@Override
	protected Job createJob() throws Exception {
		Path fastaPath = new Path(fastaFileName);
		Path outputPath = new Path(outputFileName);
		Path qualPath = null;
		
		if (useQualFile) {
			qualPath = new Path(qualFileName);
		}
		
		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());
			if (fs.exists(outputPath)) {
				log.info(String.format("Removing '%s'", outputPath));
				fs.delete(outputPath, true);
			}
		}

		Job job = new Job(getConf(), "GFFToSequenceFile");

		job.setJarByClass(GTFToSequenceFile.class);
		job.setInputFormatClass(FastaInputFormat.class);

		if (useQualFile) {
			FastaInputFormat.setInputPaths(job, fastaPath, qualPath);
		} else {
			FastaInputFormat.setInputPaths(job, fastaPath);
		}

		job.setMapperClass(CopyMapperWithId.class);
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,
				CompressionCodec.class);
		getConf().setStrings("mapred.output.compression.type", "BLOCK");

		if (useQualFile) {
			job.setReducerClass(MergeReducer.class);
		} else {
			log.info("Using Contant Quality Qual Value");
			job.setReducerClass(ConstantQualReducer.class);
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new GTFToSequenceFile(), args);
		System.exit(ret);
	}
}