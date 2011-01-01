package com.lifetech.utils.progs;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

public class SyncFasta {
	private static Logger log = Logger.getLogger(SyncFasta.class);

	private Options buildOptions() {
		Options options = new Options();
		options.addOption("i","input", true, "Input file");
		options.addOption("o","output", true, "Output file");
		options.addOption("r","reference", true, "Reference file");

		return options;
	}
	
	int recordsCount = 0;
	
	public void populateDB(File input,Connection conn) throws Exception {
		PreparedStatement createTable = conn.prepareStatement("create table seqs (name varchar(1024) primary key)");
		
		createTable.execute();
		conn.commit();

		PreparedStatement insertStmt = conn.prepareStatement("insert into seqs(name) values(?)");
		
		LineIterator it = FileUtils.lineIterator(input, "ASCII");
	
		recordsCount = 0;
		
		while(it.hasNext()) {
			String line = it.nextLine();
			if (line.startsWith(">")) {
				insertStmt.setString(1, line);
				insertStmt.execute();
				recordsCount++;
				if (recordsCount % 1000 == 0) {
					System.out.print(".");
					System.out.flush();
				}
				if (recordsCount % 50000 == 0) {
					System.out.printf(" %5dk\n",recordsCount/1000);
					System.out.flush();					
				}
			}
		}
		log.info(String.format("Total records in ref fasta = %d",recordsCount));
	}

	
	private void syncFile(Connection conn, File input, File output) throws Exception {
		PreparedStatement queryStmt = conn.prepareStatement("select name from seqs where name=?");
		PrintStream out = new PrintStream(output);
		
		LineIterator it = FileUtils.lineIterator(input, "ASCII");
		boolean printLine = false;
		int syncCount = 0;
		
		while(it.hasNext()) {
			String line = it.nextLine();
			if (line.startsWith(">")) {
				queryStmt.setString(1, line);
				queryStmt.execute();
				printLine = queryStmt.getResultSet().next();
				if (syncCount == recordsCount) break;
				syncCount++;				
				if (syncCount % 1000 == 0) {
					System.out.print(".");
					System.out.flush();
				}
				if (syncCount % 50000 == 0) {
					System.out.printf(" %5dk (%.2f)\n",syncCount/1000,syncCount*100.0/recordsCount);
					System.out.flush();					
				}				
			}
			if (printLine) {
				out.println(line);
			}
		}		
		log.info(String.format("Total records in output fasta = %d",syncCount));
	}
	
	private Connection getConn(String filename) throws Exception {
		Class.forName("org.h2.Driver");
		String connStr = String.format("jdbc:h2:%s",filename);
		log.info("Connecting to " + connStr);
		Connection conn = DriverManager.getConnection(connStr, "sa", "");
		
		return conn;
	}
	
	public int execute(String refFilename,String inputFilename,String outputFilename) throws Exception {
		File dbFile = new File(refFilename + ".h2.db");
		File dbFile2 = new File(refFilename + ".trace.db");

		File refFile = new File(refFilename);
		File input = new File(inputFilename);
		File output = new File(outputFilename);
		
		log.info("Reference File " + refFile.getAbsolutePath());
		log.info("Input File " + input.getAbsolutePath());
		log.info("Output File " + output.getAbsolutePath());
		
		if (dbFile.exists()) {
			dbFile.delete();
			dbFile2.delete();
		}
		
		Connection conn = null;
		
		try {
			conn = getConn(refFile.getAbsolutePath());
			populateDB(refFile, conn);
			syncFile(conn,input,output);
		}
		finally {
			if (conn != null) conn.close();
			//dbFile.delete();
			//dbFile2.delete();
		}
		
		return 0;
	}

	public int run(String[] args) throws Exception {		
		Options options = buildOptions();
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args);

		String inputFilename = "";		
		String outputFilename = "";
		String refFilename = "";

		if (cmd.hasOption("i")) {
			inputFilename = cmd.getOptionValue("i");
		} else {
			help(options);
		}

		if (cmd.hasOption("o")) {
			outputFilename = cmd.getOptionValue("o");
		} else {
			help(options);
		}

		if (cmd.hasOption("r")) {
			refFilename = cmd.getOptionValue("r");
		} else {
			help(options);
		}
		
		return execute(refFilename,inputFilename,outputFilename);
	}
	
	private void help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( appName(), options );
		System.exit(-1);
	}

	private String appName() {
		return "syncFasta";
	}
	
	public static void main(String[] args) {
		SyncFasta prog = new SyncFasta();
		try {
			System.exit(prog.run(args));
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
