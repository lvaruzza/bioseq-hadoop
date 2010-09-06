package com.lifetech.hadoop.bioseq.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;

import com.lifetech.hadoop.mapreduce.input.FastaRecordReader;
import static java.lang.System.out;


public class TestFastRecordReader {
	
	private TaskAttemptContext context;
	private Configuration config;
	private FileSystem fs;
	private static Path testFile = new Path("data/test1/input.fasta");
	
	@Before
	public void initialize() throws IOException {
		config = new Configuration();
		fs = FileSystem.get(config);
		context = new TaskAttemptContext(config,
				new TaskAttemptID());
	}
	
	@Test
	public void testCreation() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader();

		InputSplit split = new FileSplit(testFile,0,100,null);
		frr.initialize(split,context);
	}
	
	@Test
	public void testWholeFileReadOne() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader();
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		frr.nextKeyValue();

		out.println("key   = " + frr.getCurrentKey());
		out.println("value = " + frr.getCurrentValue());
		
		/*while(frr.nextKeyValue()) {
			out.println(frr.getCurrentKey());
			out.println(frr.getCurrentValue());
		}*/
	}

	@Test
	public void testWholeFileReadAll() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader();
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		int i = 0;
		out.println("*********************************************************************");
		while(frr.nextKeyValue()) {
			out.println("++++++++++++++++++++++");
			out.println(String.format("key %d  = |%s|",i,frr.getCurrentKey()));
			out.println(String.format("value %d = |%s| ",i,frr.getCurrentValue()));
			out.println("++++++++++++++++++++++");
			i++;
		}
		out.println("*********************************************************************");
	}
	
}
