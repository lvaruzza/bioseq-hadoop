package com.lifetech.hadoop.bioseq.converters;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class TestFastaToFastq {

	@Test
	public void testMapper() throws IOException, InterruptedException {
		FastaToFastq.CopyMapperWithId mapper = new FastaToFastq.CopyMapperWithId();
		BioSeqWritable value = new BioSeqWritable();
		value.set("test", "T0123012", null);

		Configuration config = new Configuration();
		Mapper.Context context = mock(Mapper.Context.class);
		when(context.getConfiguration()).thenReturn(config);

		mapper.map(null, value, context);

		verify(context,times(1)).write(new Text("test"),value);
	}
	
	@Test
	public void testReducer() throws IOException, InterruptedException {
		FastaToFastq.MergeReducer reducer = new FastaToFastq.MergeReducer();

		Configuration config = new Configuration();
		Reducer.Context context = mock(Reducer.Context.class);
		when(context.getConfiguration()).thenReturn(config);

		Text key = new Text("test");
		BioSeqWritable seq = new BioSeqWritable("test","T0123",null);
		BioSeqWritable qual = new BioSeqWritable("test",null,new byte[] {0,10,10,10});
		List<BioSeqWritable> values = Arrays.asList(seq,qual);
		
		reducer.reduce(key, values, context);
		verify(context).write(new Text(""), 
				new BioSeqWritable(key,seq.getSequence(),qual.getQuality()));

	}
}
