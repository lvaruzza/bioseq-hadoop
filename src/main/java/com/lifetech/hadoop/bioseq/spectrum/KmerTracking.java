package com.lifetech.hadoop.bioseq.spectrum;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class KmerTracking extends ArrayWritable {

	public KmerTracking() {
		super(KmerOrig.class);
	}

	public void addAll(KmerTracking kmer) {
		Writable[] a = this.get();
		Writable[] b = kmer.get();
		Writable[] r =  (Writable[])ArrayUtils.addAll(a,b);
		
		this.set(r);
	}

	public void set(Text id, int i) {
		Writable[] r=new Writable[1];
		r[0] = new KmerOrig(id,i);
		this.set(r);
	}

}
