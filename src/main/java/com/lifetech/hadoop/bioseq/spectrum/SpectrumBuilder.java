package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
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
import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.bioseq.FourBitsEncoder;
import com.lifetech.hadoop.bioseq.converters.FastaToFastq;

public class SpectrumBuilder extends Configured implements Tool {
    private static Logger log = Logger.getLogger(FastaToFastq.class);	
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
	
	private String inputFile;	
	private String outputFile;
	private boolean removeOldOutput = false;
	private int kmerSize = 17;
	private boolean doReverse = false;
	private boolean doComplement = true;
	private int leftTrim = 0;
	private int rightTrim = 0;
	
	private void parseCmdLine(String[] args) throws ParseException {
		// create Options object
		Options options = new Options();

		// add t option
		options.addOption("i","input", true, "Fasta/csfasta input file");
		options.addOption("o","output", true, "Output fastq file");
		options.addOption("removeOutput", false, "Remove old output");

		options.addOption("k","kmerSize", true, "kmer Size");
		options.addOption("R","reverse", false, "Also use the reverse sequence in the spectrum");
		options.addOption("nc","noComplement", false, "Do not do the complement when doing the reverse");
		options.addOption("lt","leftTrim", true, "Trim the read on left");
		options.addOption("rt","rightTrim", true, "Trim the read on right");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);
		
		if (cmd.hasOption("i")) {
			inputFile = cmd.getOptionValue("i");
			log.info(String.format("Input file '%s'", inputFile));
		} else {
			log.error(String.format("Missing mandatory argument -i / --input"));			
			help(options);
			exit(-1);
		}
		
		
		if (cmd.hasOption("o")) {
			outputFile = cmd.getOptionValue("o");
			log.info(String.format("Output path '%s'", outputFile));
		} else {
			log.error(String.format("Missing mandatory argument -o / --output"));			
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
	
		if (cmd.hasOption("removeOutput")) {
			removeOldOutput=true;
		} else {
			removeOldOutput=false;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		parseCmdLine(args);
		
		Path inputPath = new Path(inputFile);
		Path outputPath = new Path(outputFile);

		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());
			log.info(String.format("Removing '%s'", outputFile));
			fs.delete(outputPath, true);
		}

		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,CompressionCodec.class);
		getConf().setStrings("mapred.output.compression.type", "BLOCK");
		getConf().setInt("spectrum.k", kmerSize);
		getConf().setInt("spectrum.leftTrim", leftTrim);
		getConf().setInt("spectrum.rightTrim", rightTrim);
		getConf().setBoolean("spectrum.doReverse", doReverse);
		
		Job job = new Job(getConf(), "spectrumBuild");

		job.setJarByClass(SpectrumBuilder.class);

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

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SpectrumBuilder(), args);
		System.exit(ret);
	}
}
