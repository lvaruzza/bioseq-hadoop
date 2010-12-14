package com.lifetech.hadoop.bioseq.stats;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoCodec;
import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.bioseq.FourBitsEncoder;

public class SequenceDist extends CLIApplication implements Tool {
    private static Logger log = Logger.getLogger(SequenceDist.class);	
	private static BioSeqEncoder encoder = new FourBitsEncoder();
	
	public static class BuilderMapper extends
			Mapper<Text, BioSeqWritable, BytesWritable, IntWritable> {

		//private BytesWritable kmer = new BytesWritable();
		private IntWritable ONE = new IntWritable(1);

        
		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			int k = context.getConfiguration().getInt("spectrum.k", 17);
			int leftTrim = context.getConfiguration().getInt("spectrum.leftTrim", 0);
			int rightTrim = context.getConfiguration().getInt("spectrum.rightTrim", 0);
			
			boolean doReverse = context.getConfiguration().getBoolean("spectrum.doReverse", false);
			boolean doComplement = context.getConfiguration().getBoolean("spectrum.doReverse", true);
			
			Text seq = value.getSequence();
			int size = seq.getLength();
			byte[] data = seq.getBytes();
			for (int i = leftTrim; i < size - k + 1 -rightTrim; i++) {
				byte [] r=encoder.encode(data, i, k);

				/*System.out.println("encoded: ");
				FourBitsEncoder.printBytes(r);
				System.out.println();
				FourBitsEncoder.printBytes(encoder.reverse(r));
				System.out.println();*/
				
				//kmer.set(r,0,r.length);
				//context.write(kmer, ONE);
				
				context.write(new BytesWritable(r), ONE);
				if (doReverse) {
					if (doComplement) {
						context.write(new BytesWritable(encoder.complement(encoder.reverse(r))), ONE);						
					} else {
						context.write(new BytesWritable(encoder.reverse(r)), ONE);
					}
				}
			}
		}
	}

	public static class MergeReducer extends
			Reducer<BytesWritable, IntWritable, BytesWritable, IntWritable> {

		public void reduce(BytesWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			
			for (IntWritable count : values) {
				sum += count.get(); 
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	private String inputFileName;	
	private int kmerSize = 17;
	private boolean doReverse = false;
	private boolean doComplement = true;
	private int leftTrim = 0;
	private int rightTrim = 0;

	protected Options buildOptions() {
		// create Options object
		Options options = new Options();

		// add t option
		options.addOption("i","input", true, "SequenceFile input file");

		options.addOption("k","kmerSize", true, "kmer Size");
		options.addOption("R","reverse", false, "Also use the reverse sequence in the spectrum");
		options.addOption("nc","noComplement", false, "Do not do the complement when doing the reverse");
		options.addOption("lt","leftTrim", true, "Trim the read on left");
		options.addOption("rt","rightTrim", true, "Trim the read on right");
		
		addOutputOptions(options);
		
		return options;
	}
	
	protected void checkCmdLine(Options options, CommandLine cmd) {		
		if (cmd.hasOption("i")) {
			inputFileName = cmd.getOptionValue("i");
			log.info(String.format("Input file '%s'", inputFileName));
		} else {
			log.error(String.format("Missing mandatory argument -i / --input"));			
			help(options);
			exit(-1);
		}
		
		
		if (cmd.hasOption("k")) {
			kmerSize = Integer.parseInt(cmd.getOptionValue("k"));
			log.info(String.format("Kmer size = %d",kmerSize));
		}

		if (cmd.hasOption("lt")) {
			leftTrim = Integer.parseInt(cmd.getOptionValue("lt"));
			log.info(String.format("Left Trim = %d",leftTrim));
		}

		if (cmd.hasOption("rt")) {
			rightTrim = Integer.parseInt(cmd.getOptionValue("rt"));
			log.info(String.format("Right Trim = %d",rightTrim));
		}
		
		if (cmd.hasOption("R")) {
			doReverse = true;
			log.info(String.format("Doing the reverse sequence in Spectrum"));
		}

		if (cmd.hasOption("nc")) {
			doComplement = false;
			log.info(String.format("NOT Doing the complment when doing the reverse in Spectrum"));
		}
	
		this.checkOutputOptionsInCmdLine(options, cmd);		
	}
	
	@Override
	protected Job createJob() throws Exception {
		Path inputPath = new Path(inputFileName);
		Path outputPath = new Path(outputFileName);

		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());
			log.info(String.format("Removing '%s'", outputFileName));
			fs.delete(outputPath, true);
		}

		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,CompressionCodec.class);
		getConf().setStrings("mapred.output.compression.type", "BLOCK");
		getConf().setInt("spectrum.k", kmerSize);
		getConf().setInt("spectrum.leftTrim", leftTrim);
		getConf().setInt("spectrum.rightTrim", rightTrim);
		getConf().setBoolean("spectrum.doReverse", doReverse);
		getConf().setBoolean("spectrum.doComplement", doComplement);
		
		Job job = new Job(getConf(), "spectrumBuild");

		job.setJarByClass(SequenceDist.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(BuilderMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setCombinerClass(MergeReducer.class);
		
		job.setReducerClass(MergeReducer.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SequenceDist(), args);
		System.exit(ret);
	}
}
