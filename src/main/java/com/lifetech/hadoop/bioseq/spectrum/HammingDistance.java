package com.lifetech.hadoop.bioseq.spectrum;


public class HammingDistance extends Distance {

	@Override
	public double distance(byte[] s1, int start1, int size1, byte[] s2,
			int start2, int size2) {
		
		int size = Math.min(size1, size2);
		int distance = 0;
		for(int i=0;i<size;i++) {
			distance += (s1[start1+i] == s2[start2+i]) ? 0 : 1;
		}
		
		return distance;
	}

	@Override
	public double distanceOfEncoded(byte[] s1, int start1, int size1, byte[] s2,
			int start2, int size2) {
		int size = Math.min(size1, size2);
		int distance = 0;
		for(int i=0;i<size;i++) {
			distance += ((s1[start1+i] & 0xf0) == (s2[start2+i] & 0xf0)) ? 0 : 1;
			
			if ((s1[start1+i] & 0x0f) != 0x0f) {
				distance += ((s1[start1+i] & 0x0f) == (s2[start2+i] & 0x0f)) ? 0 : 1;				
			}
		}
		
		return distance;
	}

}
