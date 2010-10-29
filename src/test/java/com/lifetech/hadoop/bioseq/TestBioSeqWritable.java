package com.lifetech.hadoop.bioseq;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

public class TestBioSeqWritable {

	@Test
	public void testSeqType() {
		BioSeqWritable seq = new BioSeqWritable();
		seq.set("a","ACGT", null);
		assertEquals(BioSeqWritable.BioSeqType.SequenceOnly,seq.getType());

		seq.set("a",null, new byte[] {10,12,20,30});
		assertEquals(BioSeqWritable.BioSeqType.QualityOnly,seq.getType());

		seq.set("a",null, null);
		assertEquals(BioSeqWritable.BioSeqType.Empty,seq.getType());

		seq.set("a","ACGT",new byte[] {10,12,20,30});
		assertEquals(BioSeqWritable.BioSeqType.Complete,seq.getType());
		
	}
	
	@Test
	public void testSeqTypeAfterSerialization() throws IOException {
		DataOutputBuffer output = new DataOutputBuffer();
		
		BioSeqWritable seq = new BioSeqWritable();
		
		seq.set("a","ACGT",new byte[] {10,12,20,30});
		seq.write(output);
		byte[] data = output.getData();
		
		DataInputBuffer input = new DataInputBuffer();
		input.reset(data, data.length);

		BioSeqWritable seq2 = new BioSeqWritable();		
		seq2.readFields(input);		
		assertEquals(BioSeqWritable.BioSeqType.Complete,seq2.getType());
		
		
	}
}
