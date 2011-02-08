package com.lifetech.hadoop.bioseq.stats;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestQualityMedianStatistics {

	@Test
	public void testQualityCutoff() throws IOException {
		QualityMedianStatistics qms = new QualityMedianStatistics();
		qms.setConf(new Configuration());
		String home = System.getenv("HOME");
		
		Path input = new Path("file://"  + home + "/workspace/bioseq/data/medianStat/*");
		
		int cutoff = qms.qualityCutoff(input, 0.25f);
		System.out.println(cutoff);
	}
}
