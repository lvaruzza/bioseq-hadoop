package com.lifetech.utils.progs;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	private static int BREAK1 = 2000;
	private static int BREAK2 = 50*BREAK1;
	
	public static interface NameExtractor {
		public String extract(String line);
	}
	
	public static class IdExtractor implements NameExtractor {
		@Override
		public String extract(String line) {
			return line;
		}		
	}

	public static class SOLiDExtractor implements NameExtractor {
		private static Pattern regexp = Pattern.compile(">(\\d+_\\d+_\\d+)_[FR][35]");
		@Override
		public String extract(String line) {
			Matcher matcher = regexp.matcher(line);
			if (matcher.matches()) {
				return matcher.group(1);
			} else {
				throw new RuntimeException("Invalid header '" + line + "'");
			}
		}		
	}
	
	private NameExtractor extractor = new IdExtractor();
	
	
	private int recordsCount = 0;
	private DateFormat elapsedFormat = new SimpleDateFormat("kk:mm:ss.SSSS");
	
	private Connection getConn(String filename) throws Exception {
		Class.forName("org.h2.Driver");
		String connStr = String.format("jdbc:h2:%s",filename);
		log.info("Connecting to " + connStr);
		Connection conn = DriverManager.getConnection(connStr, "sa", "");
		
		return conn;
	}


	public void populateDB(File input,Connection conn) throws Exception {
		PreparedStatement createTable = conn.prepareStatement("create table seqs (name varchar(1024) primary key)");
		
		createTable.execute();
		conn.commit();

		PreparedStatement insertStmt = conn.prepareStatement("insert into seqs(name) values(?)");
		
		LineIterator it = FileUtils.lineIterator(input, "ASCII");
	
		recordsCount = 0;
		long startTime = System.currentTimeMillis();
		
		while(it.hasNext()) {
			String line = it.nextLine();
			if (line.startsWith(">")) {
				insertStmt.setString(1, extractor.extract(line));
				insertStmt.execute();
				recordsCount++;
				if (recordsCount % BREAK1 == 0) {
					System.out.print(".");
					System.out.flush();
				}
				if (recordsCount % BREAK2 == 0) {
					long curTime = System.currentTimeMillis() - startTime;
					
					System.out.printf(" %5dk. Elapsed %s (%.2f seqs/s)\n",
							recordsCount/1000,
							elapsedFormat.format(new Date(curTime)),
							recordsCount*1000.0/curTime);
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
		
		long startTime = System.currentTimeMillis();

		while(it.hasNext()) {
			String line = it.nextLine();
			if (line.startsWith(">")) {
				queryStmt.setString(1, extractor.extract(line));
				queryStmt.execute();
				printLine = queryStmt.getResultSet().next();
				if (syncCount == recordsCount) break;
				syncCount++;				
				if (syncCount % BREAK1 == 0) {
					System.out.print(".");
					System.out.flush();
				}
				if (syncCount % BREAK2 == 0) {
					long curTime = System.currentTimeMillis() - startTime;

					System.out.printf(" %5dk (%.2f%%). Elapsed %s (%.2f seqs/s)\n",
							syncCount/1000,
							syncCount*100.0/recordsCount,
							elapsedFormat.format(new Date(curTime)),
							syncCount*1000.0/curTime);
					System.out.flush();					
				}				
			}
			if (printLine) {
				out.println(line);
			}
		}		
		log.info(String.format("Total records in output fasta = %d",syncCount));
	}
	
	private Options buildOptions() {
		Options options = new Options();
		options.addOption("i","input", true, "Input file");
		options.addOption("o","output", true, "Output file");
		options.addOption("r","reference", true, "Reference file");
		options.addOption("n","nameScheme", true, "naming scheme of sequences (solid or all)");

		return options;
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

		String inputFilename = null;		
		String outputFilename = null;
		String refFilename = null;

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

		if (cmd.hasOption("n")) {
			String scheme = cmd.getOptionValue("n");
			if (scheme.equals("solid")) {
				extractor = new SOLiDExtractor();
			}
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
