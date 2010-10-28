package com.lifetech.hadoop.bioseq.stats;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

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
	
	@Test
	public void testSerialization() throws IOException {
		ValueStatsWritable vs = new ValueStatsWritable();
		DataOutputBuffer out = new DataOutputBuffer();
		DataInputBuffer in = new DataInputBuffer();
		
		vs.update(1);
		vs.update(2);
		vs.update(4);
		vs.write(out);
		vs.update(1024);
		byte[] data = out.getData();
		in.reset(data, data.length);
		
		vs.readFields(in);
		vs.update(8);
		vs.update(16);
		
		assertEquals(6.2,vs.mean(),1e-15);
		assertEquals(37.2,vs.variance(),1e-15);
	}
	
}
