package com.lifetech.hadoop.bioseq.spectrum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Kmer origin (read name and position)
 */

public class KmerOrig implements Writable {

	private Text readName;
	private IntWritable position;
	
	public KmerOrig() {
		this.readName = new Text();
		this.position = new IntWritable();
	}
	
	public KmerOrig(Text readName, int position) {
		this.readName = readName;
		this.position = new IntWritable(position);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		readName.readFields(in);
		position.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		readName.write(out);
		position.write(out);
	}

	public Text getReadName() {
		return readName;
	}

	public void setReadName(Text readName) {
		this.readName = readName;
	}

	public IntWritable getPosition() {
		return position;
	}

	public void setPosition(IntWritable position) {
		this.position = position;
	}	
}
