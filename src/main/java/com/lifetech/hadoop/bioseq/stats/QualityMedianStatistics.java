package com.lifetech.hadoop.bioseq.stats;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.uncommons.maths.statistics.DataSet;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class QualityMedianStatistics extends CLIApplication implements Tool {
    private static Logger log = Logger.getLogger(QualityMedianStatistics.class);

	public static class QualityMapper extends
			Mapper<Writable, BioSeqWritable, ByteWritable, LongWritable> {

		private static LongWritable ONE = new LongWritable(1);
		private ByteWritable medianQual = new ByteWritable();
		
		public void map(Writable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			byte[] quals = value.getQuality().getBytes();
			int len = value.getQuality().getLength();
			
			DataSet stats = new DataSet();
			
			for(int i=0;i<len;i++) {
				stats.addValue(quals[i]);
			}
			medianQual.set((byte) stats.getMedian());
			context.write(medianQual, ONE);
		}
	}
	
	public static class StatisticsReducer
			extends
			Reducer<ByteWritable, LongWritable, ByteWritable, LongWritable> {

		private LongWritable result = new LongWritable();
		
		public void reduce(ByteWritable key,
				Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	private long loadFile(SortedMap<Integer,Long> map,Path file,FileSystem fs) throws IOException {
		SequenceFile.Reader reader = null;
		long sum = 0;
		try {	
			log.info("qualityCutoff: Reading file " + file);
			reader = new SequenceFile.Reader(fs, file, getConf());
			ByteWritable key = new ByteWritable();
			LongWritable value = new LongWritable();
			
			while (reader.next(key, value)) {
				map.put(- key.get(), value.get());
				sum += value.get();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		return sum;
	}
	
	public int qualityCutoff(Path input,float quantile) throws IOException {
		int cutoff = 20;
		
		FileSystem fs = input.getFileSystem(getConf());		
		TreeMap<Integer,Long> map = new TreeMap<Integer,Long>();
		FileStatus[] files = fs.globStatus(input, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				System.out.println(path.getName());
				return path.getName().startsWith("part-r");
			}
		});
		
		long sum = 0;
		
		for (FileStatus file : files) {
			if (!file.isDir()) {
				sum += loadFile(map,file.getPath(),fs);
			}
		}
		
		long valueCutoff = (int)(quantile*sum);
		long partialSum = 0;
		
		for(Entry<Integer,Long> entry : map.entrySet()) {
			partialSum += entry.getValue();
			System.out.println(String.format("%d\t%d\t%.2f",
					-entry.getKey(),entry.getValue(),partialSum*100.0/sum));
			
			if (partialSum >= valueCutoff) {
				cutoff = - entry.getKey();
				break;
			}
		}
		
		return cutoff;
	}
	
	@Override
	protected Job createJob() throws Exception {
		Path inputPath = new Path(this.inputFileName);
		Path outputPath = new Path(this.outputFileName);

		Job job = new Job(getConf(), appName());

		maybeRemoevOldOutput(outputPath);
		
		switch(inputFormat) {
		case FASTA: 
			job.setInputFormatClass(FastaInputFormat.class);
			FastaInputFormat.setInputPaths(job, inputPath);
			break;
		case SEQUENCE_FILE:
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(job, inputPath);
			break;
		case TEXT:
			throw new RuntimeException("Input Format not implemented: " + IOFormat.TEXT);
		default:
			throw new RuntimeException("Invalid input format '" + this.inputFormat + "'");
		}
		
		job.setJarByClass(QualityMedianStatistics.class);

		job.setMapperClass(QualityMapper.class);
		job.setMapOutputKeyClass(ByteWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setCombinerClass(StatisticsReducer.class);
		job.setReducerClass(StatisticsReducer.class);

		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(LongWritable.class);

		switch(outputFormat) {
		case TEXT:
			job.setOutputFormatClass(TextOutputFormat.class);
			FastqOutputFormat.setOutputPath(job, outputPath);
			break;
		case SEQUENCE_FILE:
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, outputPath);
			break;
		default:
			throw new RuntimeException("Output Format not implemented or invalid: " + outputFormat);			
		}
		return job;
	}

	@Override
	protected Options buildOptions() {
		Options options = new Options();

		addInputOptions(options);
		addOutputOptions(options);
		addInputFormatOptions(options);
		addInputOptions(options);
		
		return options;
	}


	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		checkInputOptionsInCmdLine(options, cmd);
		checkOutputOptionsInCmdLine(options, cmd);
		checkInputFormatInCmdLine(options, cmd);
		checkOutputFormatInCmdLine(options, cmd);
	}


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new QualityMedianStatistics(), args);
	}


	@Override
	protected String appName() {
		return "qualityMedianStatistics";
	}


}
