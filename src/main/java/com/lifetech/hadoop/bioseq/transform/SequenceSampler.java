package com.lifetech.hadoop.bioseq.transform;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.lifetech.hadoop.CLI.CLIApplication;

public class SequenceSampler extends CLIApplication {
    private static Logger log = Logger.getLogger(SequenceSampler.class);

    private double percent;
    
   
	@Override
	protected Options buildOptions() {
		Options options = new Options();
		addOutputOptions(options);
		addInputOptions(options);
		
		options.addOption("p", "percentAccept", true, "percentage of reads to be accespted");
		return options;
	}
    
	@Override
	protected void checkCmdLine(Options options, CommandLine cmd) {
		this.checkOutputOptionsInCmdLine(options, cmd);
		this.checkInputOptionsInCmdLine(options, cmd);
		
		if(cmd.hasOption("p")) {
			percent = Double.parseDouble(cmd.getOptionValue("p"));
		} else {
			log.error(String.format("Missing mandatory argument -p / --percentAccept"));			
			help(options);
			exit(-1);			
		}
	}

	@Override
	protected Job createJob() {
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
}
