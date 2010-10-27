package com.lifetech.hadoop.bioseq.stats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class ValueStatsWritable {
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
}
