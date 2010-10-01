package com.lifetech.hadoop.bioseq.mapreduce.input;

import static org.junit.Assert.assertEquals;

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

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaRecordReader;
import com.lifetech.utils.FastqUtils;


public class TestFastaRecordReader {
	
	private TaskAttemptContext context;
	private Configuration config;
	private FileSystem fs;
	private static Path testFile = new Path("data/test1/input.fasta");
	private static Path testQualFile = new Path("data/fastaqual/F3.qual");
	
	@Before
	public void initialize() throws IOException {
		config = new Configuration();
		fs = FileSystem.get(config);
		context = new TaskAttemptContext(config,
				new TaskAttemptID());
	}
	
	@Test
	public void testCreation() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader(true);

		InputSplit split = new FileSplit(testFile,0,100,null);
		frr.initialize(split,context);
	}
	
	@Test
	public void testWholeFile_testFirst() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader(true);
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		frr.nextKeyValue();
		assertEquals(BioSeqWritable.BioSeqType.SequenceOnly,frr.getCurrentValue().getType());
		
		assertEquals(new BioSeqWritable("487_14_960_R3","G20112231312123121221311132212223221221222322122222",null),
				frr.getCurrentValue());
	}

	@Test
	public void testWholeFile_testLast() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader(true);
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);
		int i = 0;
		BioSeqWritable value = null;
		while(frr.nextKeyValue()) {
			value = frr.getCurrentValue();
			//System.out.println(String.format("*%s*",value.toString()));
			i++;
			//System.out.println("\n############################################################\n");
		}
		assertEquals(495,i);
		assertEquals("487_70_1270_R3\tG31232233030221120330213013113201232012023333001233",value.toString());
	}
	
	@Test
	public void testWholeFile_testFourth() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader(true);
		long size = fs.getFileStatus(testFile).getLen();
		InputSplit split = new FileSplit(testFile,0,size,null);

		frr.initialize(split,context);

		for(int i=0;i<4;i++) 
			frr.nextKeyValue();
			
		BioSeqWritable value = frr.getCurrentValue();
		
		assertEquals(new BioSeqWritable("487_15_417_R3",
				"G232333300323303133222311210012332211322220221112220312021222322221322" + 
				"3102220212220222222222022222222G10111121320333212211121121213122221212212222222222",null),value);
	}
	
	@Test
	public void testSplitFile() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader(true);
		long size = fs.getFileStatus(testFile).getLen();
		BioSeqWritable value = null;

		InputSplit split1 = new FileSplit(testFile,0,size/2,null);
		frr.initialize(split1,context);
		int i=0;
		
		while(frr.nextKeyValue()) {
			value = frr.getCurrentValue();
			//System.out.println(String.format("%d %d *%s*",i,value.getLength(),value.toString()));
			i++;
		}
		assertEquals("487_45_258_R3\tG22112101302323223132123132130222222222222322222222",value.toString());
		
		//System.out.println("\n############################################################\n");

		InputSplit split2 = new FileSplit(testFile,size/2,size,null);
		frr.initialize(split2,context);
		
		frr.nextKeyValue();
		value = frr.getCurrentValue();
		assertEquals("487_45_443_R3\tG20311313031332313131133121313023122231322222222222",value.toString());
		i++;
		
		while(frr.nextKeyValue()) {
			value = frr.getCurrentValue();
			//System.out.println(String.format("%d %d *%s*",i,value.getLength(),value.toString()));
			i++;
		}
		
		assertEquals(495,i);
		assertEquals("487_70_1270_R3\tG31232233030221120330213013113201232012023333001233",value.toString());		
	}		

	@Test
	public void testWholeFile_Qual_testFirst() throws IOException, InterruptedException {
		FastaRecordReader frr = new FastaRecordReader(true);
		long size = fs.getFileStatus(testQualFile).getLen();
		InputSplit split = new FileSplit(testQualFile,0,size,null);

		frr.initialize(split,context);
		frr.nextKeyValue();

		assertEquals(BioSeqWritable.BioSeqType.QualityOnly,frr.getCurrentValue().getType());		
		assertEquals(new BioSeqWritable("469_26_42_F3",null,
				FastqUtils.fastqBinary("!4))+()1+&1').'.(&046&',&&*&)'%'&&'&&4(+((&')8&-%/*")),
				frr.getCurrentValue());
	}


}
