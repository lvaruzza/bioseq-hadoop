package com.lifetech.hadoop.bioseq.mapreduce.input;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;

import com.lifetech.hadoop.mapreduce.input.FastaRecordReader;
import com.lifetech.hadoop.mapreduce.output.FastaRecordWriter;


public class TestFastaRecordWriter {
	
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
	public void testWholeFile_testFirst() throws IOException, InterruptedException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		FastaRecordReader frr = new FastaRecordReader();
		FastaRecordWriter<NullWritable> frw = new FastaRecordWriter<NullWritable>(new DataOutputStream(out));
		
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		frr.nextKeyValue();
		frw.write(NullWritable.get(), frr.getCurrentValue());
		assertEquals(">487_14_960_R3\nG20112231312123121221311132212223221221222322122222\n",out.toString());
	}
}
