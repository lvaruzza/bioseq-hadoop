package com.lifetech.hadoop.bioseq.mapreduce.input;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;

import com.lifetech.hadoop.mapreduce.input.FastaRecordReader;
import static org.junit.Assert.*;


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
	public void testWholeFile_testFirst() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader();
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		frr.nextKeyValue();
		assertEquals("487_14_960_R3\tG20112231312123121221311132212223221221222322122222",frr.getCurrentValue().toString());
	}

	@Test
	public void testWholeFile_testLast() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader();
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		int i = 0;
		Text value = null;
		while(frr.nextKeyValue()) {
			value = frr.getCurrentValue();
			//System.out.println(String.format("%d *%s*",value.getLength(),value.toString()));
			i++;
			//System.out.println("\n############################################################\n");
		}
		assertEquals(495,i);
		assertEquals("487_70_1270_R3\tG31232233030221120330213013113201232012023333001233",value.toString());
	}
	
}
