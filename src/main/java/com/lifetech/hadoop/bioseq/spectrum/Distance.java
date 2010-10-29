package com.lifetech.hadoop.bioseq.spectrum;

import org.apache.hadoop.io.*;

abstract public class Distance {
	abstract public float distance(byte[] s1,int start1,int size1,byte[] s2,int start2,int size2);

	public float distance(BytesWritable s1,BytesWritable s2) {
		return distance(s1.getBytes(),0,s1.getLength(),
				s2.getBytes(),0,s2.getLength());
	}
	
	public float distance(Text s1,Text s2) {
		return distance(s1.getBytes(),0,s1.getLength(),
				s2.getBytes(),0,s2.getLength());		
	}
	
	abstract public float distanceOfEncoded(byte[] s1,int start1,int size1,byte[] s2,int start2,int size2);
	public float distanceOfEncoded(BytesWritable s1,BytesWritable s2) {
		return distanceOfEncoded(s1.getBytes(),0,s1.getLength(),
				s2.getBytes(),0,s2.getLength());		
	}
}
