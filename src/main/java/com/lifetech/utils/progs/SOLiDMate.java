package com.lifetech.utils.progs;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
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

public class SOLiDMate {
	private static Logger log = Logger.getLogger(SyncFasta.class);

	private static int BREAK1 = 2000;
	private static int DOTSPERLINE = 50;
	private static int BREAK2 = DOTSPERLINE * BREAK1;


	private SyncFasta.NameExtractor extractor = new SyncFasta.SOLiDExtractor();

	private int recordsCount = 0;
	private DateFormat elapsedFormat = new SimpleDateFormat("KK:mm:ss");

	private Connection getConn(String filename) throws Exception {
		Class.forName("org.h2.Driver");
		String connStr = String.format("jdbc:h2:%s", filename);
		log.info("Connecting to " + connStr);
		Connection conn = DriverManager.getConnection(connStr, "sa", "");

		return conn;
	}

	public void populateRef(File input, Connection conn) throws Exception {
		log.info("Reading reference file " + input.getAbsolutePath());
		PreparedStatement createTable = conn
				.prepareStatement("create table ref (pos varchar(1024) primary key,name varchar(1024))");

		createTable.execute();
		conn.commit();

		PreparedStatement insertStmt = conn
				.prepareStatement("insert into ref(pos,name) values(?,?)");

		LineIterator it = FileUtils.lineIterator(input, "ASCII");

		recordsCount = 0;
		long startTime = System.currentTimeMillis();

		while (it.hasNext()) {
			String line = it.nextLine();
			if (line.startsWith(">")) {
				insertStmt.setInt(1, recordsCount);
				insertStmt.setString(2, extractor.extract(line));
				insertStmt.execute();
				recordsCount++;
				if (recordsCount % BREAK1 == 0) {
					System.out.print(".");
					System.out.flush();
				}
				if (recordsCount % BREAK2 == 0) {
					long curTime = System.currentTimeMillis() - startTime;

					System.out.printf(" %5dk. Elapsed %s (%.2f K seqs/s)\n",
							recordsCount / 1000,
							elapsedFormat.format(new Date(curTime)),
							recordsCount * 1.0 / curTime);
					System.out.flush();
				}
			}
		}
		long curTime = System.currentTimeMillis() - startTime;
		int rest = DOTSPERLINE - (recordsCount % DOTSPERLINE);

		System.out.printf("%" + rest + "s %5dk. Elapsed %s (%.2f K seqs/s)\n",
				"", recordsCount / 1000,
				elapsedFormat.format(new Date(curTime)), recordsCount * 1.0
						/ curTime);
		System.out.flush();
		log.info(String.format("Total records in ref fasta = %d", recordsCount));
	}

	private String inputTag;

	public void populateInput(File input, Connection conn) throws Exception {
		Pattern regexp = Pattern.compile("^>(\\d+_\\d+_\\d+)_(.*)");

		log.info("Reading Input file " + input.getAbsolutePath());
		PreparedStatement createTable = conn
				.prepareStatement("create table input (name varchar(1024) primary key,seq varchar(100))");

		createTable.execute();
		conn.commit();

		PreparedStatement insertStmt = conn
				.prepareStatement("insert into input(name,seq) values(?,?)");

		LineIterator it = FileUtils.lineIterator(input, "ASCII");

		long startTime = System.currentTimeMillis();
		int inputCount = 0;

		while (it.hasNext()) {
			String line = it.nextLine();
			if (line.startsWith(">")) {
				// Get the tag from the first sequence
				if (inputCount == 0) {
					Matcher m = regexp.matcher(line);
					if (m.matches()) {
						inputTag = m.group(2);
					}
				}

				insertStmt.setString(1, extractor.extract(line));
				if (!it.hasNext()) {
					throw new RuntimeException("Incomplete sequence at file "
							+ input.getAbsolutePath() + " in seq " + line);
				}
				String seq = it.nextLine();
				insertStmt.setString(2, seq);
				insertStmt.execute();
				inputCount++;
				if (recordsCount % BREAK1 == 0) {
					System.out.print("*");
					System.out.flush();
				}
				if (recordsCount % BREAK2 == 0) {
					long curTime = System.currentTimeMillis() - startTime;

					System.out.printf(" %5dk. Elapsed %s (%.2f K seqs/s)\n",
							inputCount / 1000,
							elapsedFormat.format(new Date(curTime)), inputCount
									* 1.0 / curTime);
					System.out.flush();
				}
			}
		}
		long curTime = System.currentTimeMillis() - startTime;
		int rest = DOTSPERLINE - (recordsCount % DOTSPERLINE);

		System.out.printf("%" + rest + "s %5dk. Elapsed %s (%.2f K seqs/s)\n",
				"", recordsCount / 1000,
				elapsedFormat.format(new Date(curTime)), recordsCount * 1.0
						/ curTime);
		System.out.flush();
		log.info(String.format("Total records in ref fasta = %d", recordsCount));
	}

	private void syncFile(Connection conn, File output) throws Exception {
		log.info(" Syncing.....");
		PreparedStatement queryStmt = conn
				.prepareStatement("select seq from input where name=?");
		PreparedStatement listStmt = conn
				.prepareStatement("select name from ref order by pos");

		PrintStream out = new PrintStream(output);

		boolean printLine = false;
		int syncCount = 0;

		listStmt.execute();
		ResultSet rs = listStmt.getResultSet();
		long startTime = System.currentTimeMillis();

		while (rs.next()) {
			String name =  rs.getString(1);
			queryStmt.setString(1, name);
			queryStmt.execute();
			ResultSet ors = queryStmt.getResultSet();
			printLine = ors.next();
			// if (syncCount == recordsCount) break;
			if (printLine) {
				syncCount++;

				if (syncCount % BREAK1 == 0) {
					System.out.print("+");
					System.out.flush();
				}
				if (syncCount % BREAK2 == 0) {
					long curTime = System.currentTimeMillis() - startTime;

					System.out.printf(
							" %5dk (%.2f%%). Elapsed %s (%.2f K seqs/s)\n",
							syncCount / 1000, syncCount * 100.0 / recordsCount,
							elapsedFormat.format(new Date(curTime)), syncCount
									* 1.0 / curTime);
					System.out.flush();
				}
				out.printf(">%s_%s\n",name,inputTag);
				out.println(ors.getString(1));
			}
		}
		long curTime = System.currentTimeMillis() - startTime;
		int rest = DOTSPERLINE - (syncCount % DOTSPERLINE);
		System.out.printf("%" + rest
				+ "s %5dk (%.2f%%). Elapsed %s (%.2f K seqs/s)\n", "",
				syncCount / 1000, syncCount * 100.0 / recordsCount,
				elapsedFormat.format(new Date(curTime)), syncCount * 1.0
						/ curTime);
		System.out.flush();
		log.info(String.format("Total records in output fasta = %d", syncCount));
	}

	private Options buildOptions() {
		Options options = new Options();
		options.addOption("i", "input", true, "Input file");
		options.addOption("o", "output", true, "Output file");
		options.addOption("r", "reference", true, "Reference file");
		return options;
	}

	public int execute(String refFilename, String inputFilename,
			String outputFilename) throws Exception {

		elapsedFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		File dbFile = new File(outputFilename + ".h2.db");
		File dbFile2 = new File(outputFilename + ".trace.db");

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
			conn = getConn(output.getAbsolutePath());
			populateRef(refFile, conn);
			populateInput(input, conn);
			syncFile(conn,output);
		} finally {
			if (conn != null)
				conn.close();
			dbFile.delete();
			dbFile2.delete();
		}

		return 0;
	}

	public int run(String[] args) throws Exception {
		Options options = buildOptions();

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

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

		return execute(refFilename, inputFilename, outputFilename);
	}

	private void help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(appName(), options);
		System.exit(-1);
	}

	private String appName() {
		return "syncFasta";
	}

	public static void main(String[] args) {
		SOLiDMate prog = new SOLiDMate();
		try {
			System.exit(prog.run(args));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
