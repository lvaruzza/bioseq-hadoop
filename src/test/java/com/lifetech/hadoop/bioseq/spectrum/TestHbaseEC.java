package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Test;

public class TestHbaseEC {

	private HbaseErrorCorrector ec; 
	
	@Before
	public void setUp() throws IOException {
		HBaseConfiguration config = new HBaseConfiguration();
		config.addResource("/usr/local/hbase/conf/hbase-site.xml");
		//ec = new HbaseErrorCorrector(config,"kmers");
	}
	
	@Test
	public void testKmerMapping() throws IOException {
		String read = "T10002323110101230032302301003030130100000031110211";
		
		//ec.mapKmers(17, read.getBytes(), 50);
	}
}
