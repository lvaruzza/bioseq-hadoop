package com.lifetech.hadoop.bioseq.transform;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.lifetech.hadoop.CLI.CLIApplication;

public class QuantileFilter extends CLIApplication implements Tool {

	private float quantile = 0.25f;
	
	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		this.checkInputOptionsInCmdLine(options, cmd);
		this.checkOutputOptionsInCmdLine(options, cmd);
		
		if (cmd.hasOption('c')) {
			quantile = Float.parseFloat(cmd.getOptionValue('c'));
		} 
	}

	@Override
	protected Options buildOptions() {
		Options options = new Options();
		
		this.addInputOptions(options);
		this.addOutputOptions(options);
		
		Option quantile = OptionBuilder.withArgName("quantile")
									   .hasArg().withLongOpt("quantile").
									   withDescription("").
									   create('c');		
		
		options.addOption(quantile);
		
		return options;
	}

	@Override
	protected Job createJob() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SequenceSampler(), args);
		System.exit(ret);
	}

	@Override
	protected String appName() {
		return "QuantileFitler";
	}	
}
