package com.lifetech.hadoop.alignment.converters;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoCodec;
import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.alignment.GFFParser;
import com.lifetech.hadoop.alignment.GFFRec;
import com.lifetech.hadoop.alignment.GeneWritable;

public class GTFToSequenceFile extends CLIApplication implements Tool {
	private static Logger log = Logger.getLogger(GTFToSequenceFile.class);

	public static class GroupByGene extends
			Mapper<LongWritable, Text, Text, GFFRec> {

		private static GFFParser parser = new GFFParser();
		private static Text GENE_ID = new Text("gene_id");
		
		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {
			
			GFFRec gff = parser.parse(line);
			
			context.write(gff.getProperty(GENE_ID), gff);
		}
	}

	public static class MergeReducer extends
			Reducer<Text, GFFRec, Text, GeneWritable> {

		private static Text TRANSCRIPT_ID = new Text("transcript_id");
		
		public void reduce(Text key, Iterable<GFFRec> values,
				Context context) throws IOException, InterruptedException {

			MapWritable transcripts = new MapWritable();
			
			for(GFFRec gff: values) {
				transcripts.put(gff.getProperty(TRANSCRIPT_ID), gff);
			}
			context.write(key, new GeneWritable(transcripts));
		}
	}



	protected Options buildOptions() {
		// create Options object
		Options options = new Options();
		
		addInputOptions(options);
		addOutputOptions(options);

		return options;
	}

	protected void checkCmdLine(Options options, CommandLine cmd) {
		checkInputOptionsInCmdLine(options, cmd);
		checkOutputOptionsInCmdLine(options, cmd);
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

		// Job
		Job job = new Job(getConf(), "GFFToSequenceFile");
		job.setJarByClass(GTFToSequenceFile.class);
		
		// Input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);
		
		// Mapper
		job.setMapperClass(GroupByGene.class);
		job.setMapOutputValueClass(GFFRec.class);
		job.setMapOutputKeyClass(Text.class);
		
		// Mapred Compression
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,
				CompressionCodec.class);
		getConf().setStrings("mapred.output.compression.type", "BLOCK");

		// Reducer
		job.setReducerClass(MergeReducer.class);
		
		// Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(GeneWritable.class);
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
