package com.lifetech.hadoop.bioseq.spectrum;

import org.apache.hadoop.io.*;

abstract public class Distance {
	abstract public double distance(byte[] s1,int start1,int size1,byte[] s2,int start2,int size2);

	public double distance(BytesWritable s1,BytesWritable s2) {
		return distance(s1.getBytes(),0,s1.getLength(),
				s2.getBytes(),0,s2.getLength());
	}

	public double distance(byte[] b1,byte[] b2) {
		return distance(b1,0,b1.length,b2,0,b2.length);
	}
	
	public double distance(String s1,String s2) {
		byte[] b1 = s1.getBytes();
		byte[] b2 = s2.getBytes();
		
		return distance(b1,0,b1.length,b2,0,b2.length);
	}
	
	public double distance(Text s1,Text s2) {
		return distance(s1.getBytes(),0,s1.getLength(),
				s2.getBytes(),0,s2.getLength());		
	}
	
	abstract public double distanceOfEncoded(byte[] s1,int start1,int size1,byte[] s2,int start2,int size2);
	public double distanceOfEncoded(BytesWritable s1,BytesWritable s2) {
		return distanceOfEncoded(s1.getBytes(),0,s1.getLength(),
				s2.getBytes(),0,s2.getLength());		
	}
}
