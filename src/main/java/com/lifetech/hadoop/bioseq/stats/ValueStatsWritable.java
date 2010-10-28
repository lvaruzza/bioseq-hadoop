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

	private static int LARGE_N = 1000000;
	private static int SMALL_DIFF = 10;
	
	public void update(ValueStatsWritable b) {
		int na = this.n.get(); 
		double m2a = this.m2.get();
		double meanA = this.mean.get();
		
		int nb = b.n.get(); 
		double m2b = b.m2.get();
		double meanB = b.mean.get();
		
		int nx = na+nb;
		double diff = meanB - meanA;
		double meanX;
		
		if (na > LARGE_N && Math.abs(na-nb) < SMALL_DIFF) {
			meanX = (meanA * na + meanB * nb)/(na+nb);			
		} else {
			meanX = meanA + (diff * nb) / nx;
		}
		
		double m2x = m2a + m2b + (diff*na*diff*nb)/nx;
		
		this.n.set(nx);
		this.m2.set(m2x);
		this.mean.set(meanX);
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
	
	@Override
	public String toString() {
		return String.format("%f +- %f",mean(),Math.sqrt(variance()));
	}
}
