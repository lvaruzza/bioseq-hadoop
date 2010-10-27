package com.lifetech.hadoop.bioseq.stats;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestValueStats {

	@Test
	public void testUpdate() {
		ValueStatsWritable vs = new ValueStatsWritable();
		
		vs.update(0);
		vs.update(5.0);
		vs.update(10);
		
		assertEquals(5,vs.mean(),1e-15);
		assertEquals(25,vs.variance(),1e-15);
	}
}
