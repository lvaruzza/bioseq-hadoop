package com.lifetech.hadoop.bioseq.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class ValueStatsWritable implements Writable {
	IntWritable n = new IntWritable(0);
	DoubleWritable mean = new DoubleWritable(0);
	DoubleWritable m2 = new DoubleWritable(0);
	
	public double variance() {
		int n = this.n.get(); 
		double m2 = this.m2.get();

		double var = m2 / (n - 1.0);
		return var;
	}
	
	public double mean() {
		return mean.get();
	}
	
	public void update(double x) {
		int n = this.n.get(); 
		double m2 = this.m2.get();
		double mean = this.mean.get();
		
		n++;
		double diff = x - mean;
		mean = mean + diff / n;
		m2 = m2 + diff * (x - mean);
			
		this.n.set(n);
		this.m2.set(m2);
		this.mean.set(mean);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		n.write(out);
		m2.write(out);
		mean.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		n.readFields(in);
		m2.readFields(in);
		mean.readFields(in);
	}

	public void update(ValueStatsWritable val) {
		this.n.set( this.n.get() + val.n.get());
		throw new RuntimeException("Unimplemented");
	}
}
