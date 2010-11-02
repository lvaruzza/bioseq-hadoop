package com.lifetech.hadoop.bioseq.spectrum;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class TestSpectrumBuilder {

	@Test
	public void testSpectrumBuilderMapper() throws IOException, InterruptedException {
		SpectrumBuilder.BuilderMapper mapper = new SpectrumBuilder.BuilderMapper();
		BioSeqWritable value = new BioSeqWritable();
		value.set("test", "T0123012", null);

		// OutputCollector<BytesWritable, IntWritable> output =
		// mock(OutputCollector.class);
		Configuration config = new Configuration();
		config.setInt("spectrum.k", 5);
		Context context = mock(Context.class);
		BioSeqEncoder encoder = BioSeqWritable.getEncoder();

		when(context.getConfiguration()).thenReturn(config);

		mapper.map(null, value, context);

		verify(context,times(1)).write(new BytesWritable(encoder.encode("01230")),new IntWritable(1));
		verify(context,times(1)).write(new BytesWritable(encoder.encode("12301")),new IntWritable(1));
		verify(context,times(1)).write(new BytesWritable(encoder.encode("23012")),new IntWritable(1));
		
		verify(context,times(1)).write(new BytesWritable(encoder.encode("03210")),new IntWritable(1));
		verify(context,times(1)).write(new BytesWritable(encoder.encode("10321")),new IntWritable(1));
		verify(context,times(1)).write(new BytesWritable(encoder.encode("21032")),new IntWritable(1));
		//verifyNoMoreInteractions(context);
	}
}
