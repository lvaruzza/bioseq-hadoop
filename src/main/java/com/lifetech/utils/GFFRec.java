package com.lifetech.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class GFFRec implements Writable {
	private Text seqname;
	private Text source;
	private Text feature;
	
	private LongWritable start;
	private LongWritable end;
	private DoubleWritable score;
	private ByteWritable strand;
	private ByteWritable frame;
	private MapWritable properties;

	@Override
	public void write(DataOutput out) throws IOException {
		seqname.write(out);
		source.write(out);
		feature.write(out);
		start.write(out);
		end.write(out);
		score.write(out);
		strand.write(out);
		frame.write(out);
		properties.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		seqname.readFields(in);
		source.readFields(in);
		feature.readFields(in);
		start.readFields(in);
		end.readFields(in);
		score.readFields(in);
		strand.readFields(in);
		frame.readFields(in);
		properties.readFields(in);
	}

}
