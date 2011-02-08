package com.lifetech.hadoop.CLI;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.sun.org.apache.commons.logging.Log;

abstract public class CLIApplication extends Configured implements Tool {
    private static Logger log = Logger.getLogger(CLIApplication.class);

    protected boolean removeOldOutput;
    protected String outputFileName;
    protected String inputFileName;
    
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
		formatter.printHelp( appName(), options );
	}
	
	protected void parseCmdLine(String[] args) throws ParseException {
		Options options = buildOptions();
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);
		
		checkCmdLine(options,cmd);
	}

	protected abstract void checkCmdLine(Options options, CommandLine cmd);
	protected abstract Options buildOptions();
	
	protected void addOutputOptions(Options options) {
		options.addOption("o","output", true, "Output file");
		options.addOption("removeOutput", false, "Remove old output");		
	}

	protected void addInputOptions(Options options) {
		options.addOption("i","input", true, "Input file");
	}

	protected void checkInputOptionsInCmdLine(Options options, CommandLine cmd) {		
		if (cmd.hasOption("i")) {
			inputFileName = cmd.getOptionValue("i");
			log.info(String.format("Input file '%s'", inputFileName));
		} else {
			log.error(String.format("Missing mandatory argument -i / --input"));			
			help(options);
			exit(-1);
		}		
	}
	
	protected void addInputFormatOptions(Options options) {
		Option option = OptionBuilder.withArgName("input-format")
								     .hasArg()
								     .withLongOpt("input-format")
								     .withDescription("Input Format")
								     .create("if");
		options.addOption(option);
	}

	protected void addOutputFormatOptions(Options options) {
		Option option = OptionBuilder.withArgName("output-format")
								     .hasArg()
								     .withLongOpt("output-format")
								     .withDescription("Output Format")
								     .create("of");
		options.addOption(option);
	}
	
	public enum IOFormat {
		FASTA,
		SEQUENCE_FILE,
		TEXT,
		INVALID_FORMAT;
		
		public static void listValidFormats(Logger log) {
			log.info("Valid Formats:\n" + 
					"\tfasta\tFasta Format\n"+
					"\tsequence\tSequence IO Format\n"+
					"\ttext\ttext Format\n");
		}
		
		public static IOFormat parseIOFormat(String name0) {
			String name = name0.toLowerCase();
			if (name.equals("fasta")) {
					return IOFormat.FASTA;
			} else if (name.equals("sequence")) {
					return IOFormat.SEQUENCE_FILE;
			} else {
				log.error(String.format("Invalid input format '%s'",name));
				IOFormat.listValidFormats(log);
				return IOFormat.INVALID_FORMAT;
			}
		}
	};
	
	protected IOFormat inputFormat = IOFormat.SEQUENCE_FILE;
	protected IOFormat outputFormat = IOFormat.SEQUENCE_FILE;


	protected void checkInputFormatInCmdLine(Options options, CommandLine cmd) {
		if(cmd.hasOption("if")) {
			inputFormat = IOFormat.parseIOFormat(cmd.getOptionValue("if"));
			if (inputFormat == IOFormat.INVALID_FORMAT)
				this.exit(-2);
		}
	}

	protected void checkOutputFormatInCmdLine(Options options, CommandLine cmd) {
		if(cmd.hasOption("of")) {
			outputFormat = IOFormat.parseIOFormat(cmd.getOptionValue("of"));
			if (outputFormat == IOFormat.INVALID_FORMAT)
				this.exit(-2);
		}
	}
	
	protected void checkOutputOptionsInCmdLine(Options options, CommandLine cmd) {
		
		if (cmd.hasOption("o")) {
			outputFileName = cmd.getOptionValue("o");
			log.info(String.format("Output file '%s'", outputFileName));
		} else {
			log.error(String.format("Missing mandatory argument -o / --output"));			
			help(options);
			exit(-1);
		}
		if (cmd.hasOption("removeOutput")) {
			removeOldOutput=true;
		} else {
			removeOldOutput=false;
		}				
	}

	protected void maybeRemoevOldOutput(Path outputPath) throws IOException {
		if (removeOldOutput) {
			FileSystem fs = outputPath.getFileSystem(getConf());		
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
		}		
	}
	
	abstract protected Job createJob() throws Exception;
	abstract protected String appName();
	
	protected void beforeMR() throws IOException {
	}

	
	private int createAndRunJob() throws Exception {
		beforeMR();
		Job job = createJob();
		
		return job.waitForCompletion(true) ? 0 : 1;		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		parseCmdLine(args);
		
		return createAndRunJob();
	}
	
	public int filter(String inputFileName,IOFormat inputFormat,
					  String outputFileName,IOFormat outputFormat,boolean removeOldOutput) throws Exception {
		
		this.inputFileName = inputFileName;
		this.outputFileName = outputFileName;
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
		this.removeOldOutput = removeOldOutput;
		
		return createAndRunJob();
	}
}
