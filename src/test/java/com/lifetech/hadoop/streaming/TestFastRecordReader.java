package com.lifetech.hadoop.streaming;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;


public class TestFastRecordReader {
	
	private Configuration config;
	private JobConf jobConf;
	
	private FileSystem fs;
	private static Path testFile = new Path("data/test1/input.fasta");
	
	@Before
	public void initialize() throws IOException {
		config = new Configuration();
		jobConf = new JobConf();
		fs = FileSystem.get(config);
	}
	
	@Test
	public void testCreation() throws IOException, InterruptedException {
		FileSplit split = new FileSplit(testFile,0,100,jobConf);

		FastaRecordReader frr = new FastaRecordReader(split,jobConf);
	}
	
	@Test
	public void testWholeFile_testFirst() throws IOException, InterruptedException {
		long size = fs.getFileStatus(testFile).getLen();
		FileSplit split = new FileSplit(testFile,0,size,jobConf);

		FastaRecordReader frr = new FastaRecordReader(split,jobConf);
		Text value=frr.createValue();
		LongWritable key =frr.createKey();
		
		frr.next(key, value);
		assertEquals("487_14_960_R3\tG20112231312123121221311132212223221221222322122222",value.toString());
	}

	@Test
	public void testWholeFile_testLast() throws IOException, InterruptedException {
		long size = fs.getFileStatus(testFile).getLen();
		FileSplit split = new FileSplit(testFile,0,size,jobConf);

		FastaRecordReader frr = new FastaRecordReader(split,jobConf);

		int i = 0;
		Text value=frr.createValue();
		LongWritable key =frr.createKey();
		while(frr.next(key,value)) {
			//System.out.println(String.format("%d *%s*",value.getLength(),value.toString()));
			i++;
			//System.out.println("\n############################################################\n");
		}
		assertEquals(495,i);
		assertEquals("487_70_1270_R3\tG31232233030221120330213013113201232012023333001233",value.toString());
	}

	
	@Test
	public void testSplitFile() throws IOException, InterruptedException {
		long size = fs.getFileStatus(testFile).getLen();

		FileSplit split1 = new FileSplit(testFile,0,size/2,jobConf);

		int i=0;
		FastaRecordReader frr = new FastaRecordReader(split1,jobConf);

		Text value=frr.createValue();
		LongWritable key =frr.createKey();
		
		while(frr.next(key,value)) {
			//System.out.println(String.format("%d %d *%s*",i,value.getLength(),value.toString()));
			i++;
		}
		assertEquals("487_45_258_R3\tG22112101302323223132123132130222222222222322222222",value.toString());
		
		//System.out.println("\n############################################################\n");

		FileSplit split2 = new FileSplit(testFile,size/2,size,jobConf);

		frr = new FastaRecordReader(split2,jobConf);
		
		frr.next(key,value);
		assertEquals("487_45_443_R3\tG20311313031332313131133121313023122231322222222222",value.toString());
		i++;
		
		while(frr.next(key,value)) {
			//System.out.println(String.format("%d %d *%s*",i,value.getLength(),value.toString()));
			i++;
		}
		
		assertEquals(495,i);
		assertEquals("487_70_1270_R3\tG31232233030221120330213013113201232012023333001233",value.toString());		
	}
}
