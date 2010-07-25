package com.lifetech.hadoop.bioseq;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFastaReader {

	private FastaReader fastaReader;
	private FileInputStream inputStream;
	
	@BeforeClass
	public void setUp() {
		try {
			inputStream = new FileInputStream("tests/test1/input.fasta");
			fastaReader = new FastaReader(inputStream);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@AfterClass
	public void shutDown() {
		try {
			inputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testReadLine() throws IOException {
		Text str = new Text();
		for (int i=0;i<2;i++) {
			fastaReader.readSeq(str);
			
			System.out.println("============================");
			System.out.println(str);
			System.out.println("============================");
		}
	}
}
