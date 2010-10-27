package com.lifetech.hadoop.bioseq.spectrum;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class SpectrumBuilderTest {

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
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
	}
}
