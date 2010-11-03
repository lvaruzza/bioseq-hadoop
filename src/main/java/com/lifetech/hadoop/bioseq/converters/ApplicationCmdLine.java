package com.lifetech.hadoop.bioseq.converters;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

abstract public class ApplicationCmdLine extends Configured {
    private static Logger log = Logger.getLogger(ApplicationCmdLine.class);

    protected boolean removeOldOutput;
    
	protected void exit(int exitStatus) {
		if (exitStatus == 0 ){
			log.info("Program successfully finishied");
		} else {
			log.info("Program finishied with ERROR!!!!");			
		}
		System.exit(exitStatus);
	}

	protected void help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "FastaToFastq", options );
	}
	
	protected void parseCmdLine(String[] args) throws ParseException {
		Options options = buildOptions();
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);
		
		checkCmdLine(options,cmd);
	}

	protected abstract void checkCmdLine(Options options, CommandLine cmd);
	protected abstract Options buildOptions();
	    
}
