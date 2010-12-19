package com.lifetech.hadoop.alignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

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

	public GFFRec(Text seqname, Text source, Text feature, LongWritable start,
			LongWritable end, DoubleWritable score, ByteWritable strand,
			ByteWritable frame, MapWritable properties) {
		super();
		this.seqname = seqname;
		this.source = source;
		this.feature = feature;
		this.start = start;
		this.end = end;
		this.score = score;
		this.strand = strand;
		this.frame = frame;
		this.properties = properties;
	}

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

	
	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append(seqname.toString()); b.append('\t');
		b.append(source.toString()); b.append('\t');
		b.append(feature.toString()); b.append('\t');
		b.append(start.toString()); b.append('\t');
		b.append(end.toString()); b.append('\t');
		if (Double.isNaN(score.get())) {
			b.append(".\t");
		} else {
			b.append(score.toString()); b.append('\t');
		}
		b.append((char)strand.get()); b.append('\t');
		b.append((char)frame.get()); b.append('\t');
		
		for( Entry<Writable,Writable> pair: properties.entrySet()) {
			b.append(pair.getKey().toString());
			b.append(' ');
			b.append(pair.getValue().toString());
			b.append("; ");
		}
		return b.toString();
	}

	public Text getSeqname() {
		return seqname;
	}

	public Text getSource() {
		return source;
	}

	public Text getFeature() {
		return feature;
	}

	public LongWritable getStart() {
		return start;
	}

	public LongWritable getEnd() {
		return end;
	}

	public DoubleWritable getScore() {
		return score;
	}

	public ByteWritable getStrand() {
		return strand;
	}

	public ByteWritable getFrame() {
		return frame;
	}

	public MapWritable getProperties() {
		return properties;
	}
}
