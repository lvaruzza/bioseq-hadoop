package com.lifetech.hadoop.bioseq.transform;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.uncommons.maths.statistics.DataSet;

import com.lifetech.hadoop.CLI.CLIApplication;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.bioseq.stats.QualityMedianStatistics;

public class QuantileFilter extends CLIApplication implements Tool {
    private static Logger log = Logger.getLogger(QuantileFilter.class);

	private float quantile = 0.25f;

	public static class QualityFilterMapper extends
			Mapper<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void map(Text key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {

			byte[] quals = value.getQuality().getBytes();
			int len = value.getQuality().getLength();
			
			int cutoff = context.getConfiguration().getInt("quality.cutoff", 20);
			
			DataSet stats = new DataSet();

			for (int i = 0; i < len; i++) {
				stats.addValue(quals[i]);
			}
			double median = stats.getMedian();
			
			if (median >= cutoff) {
				context.write(value.getId(), value);
			}
		}
	}

	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		this.checkInputOptionsInCmdLine(options, cmd);
		this.checkOutputOptionsInCmdLine(options, cmd);

		if (cmd.hasOption('p')) {
			quantile = Float.parseFloat(cmd.getOptionValue('p'));
		}
	}

	@Override
	protected Options buildOptions() {
		Options options = new Options();

		this.addInputOptions(options);
		this.addOutputOptions(options);
		options.addOption("p", "percentAccept", true,"percentage of reads to be accepted");

		return options;
	}

	@Override
	protected Job createJob() throws Exception {
		Path inputPath = new Path(this.inputFileName);
		Path outputPath = new Path(this.outputFileName);

		maybeRemoevOldOutput(outputPath);
		
		Job job = new Job(getConf(), appName());

		maybeRemoevOldOutput(outputPath);
		
		job.setJarByClass(QualityMedianStatistics.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		
		job.setMapperClass(QualityFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		parseCmdLine(args);
		
		QualityMedianStatistics qms = new QualityMedianStatistics();
		qms.setConf(getConf());
		
		String qualityDistFileName = "qms";
		
		Path qualityDistFileNamePath = new Path(qualityDistFileName);
		
		FileSystem fs = qualityDistFileNamePath.getFileSystem(getConf());
		
		if (!fs.exists(qualityDistFileNamePath)) {
			int ret = qms.filter(inputFileName, IOFormat.SEQUENCE_FILE, 
					  		     qualityDistFileName, IOFormat.SEQUENCE_FILE,false);
					
			if (ret != 0 ) {
				throw new RuntimeException("Failed Quality Distribution to file " + qualityDistFileName);
			}
		}
		
		int qualityCutoff = qms.qualityCutoff(new Path(qualityDistFileName + "/*"), quantile);
		log.info(String.format("Quality cutoff for quantily %.2f = %d", quantile,qualityCutoff));
		getConf().setInt("quality.cutoff", qualityCutoff);
		
		Job job = createJob();
		
		return job.waitForCompletion(true) ? 0 : 1;		
	}
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new QuantileFilter(), args);
		System.exit(ret);
	}

	@Override
	protected String appName() {
		return "QuantileFitler";
	}
}
