package com.lifetech.utils;

import java.util.Arrays;
import java.util.Iterator;


public class ByteArray {
	
	/*
	 * Split Iterator and Iterable 
	 */
	
	static class SplitIterator implements Iterator<byte[]> {
		private byte[] b; 
		private byte c;
		private int start; 
		private int end;

		public SplitIterator(byte[] b, byte c, int start, int len) {
			this.b = b;
			this.c = c;
			this.start =start;
			this.end = start+len;
		}

		@Override
		public boolean hasNext() {
			return (start < end);
		}

		@Override
		public byte[] next() {
			int i = start;
			for(;i<end;i++) {
				if(b[i] == c) {
					break;
				}
			}
			if (start==end) {
				return new byte[0];
			} else {
				byte[] r = Arrays.copyOfRange(b, start, i);
				start = i+1;
				return r;
			}
		}

		@Override
		public void remove() {
			throw new RuntimeException("Unimplemented");
		}
		
	}
	
	static class SpliIterable implements Iterable<byte[]> {
		private byte[] b; 
		private byte c;
		private int start; 
		private int len;
		
		public SpliIterable(byte[] b, byte c, int start, int len) {
			this.b = b;
			this.c = c;
			this.start =start;
			this.len = len;
		}

		@Override
		public Iterator<byte[]> iterator() {
			return new SplitIterator(b,c,start,len);
		}
		
	};
	
	public static byte[][] split2(byte [] b,byte c) {
		int i = 0;
		for(;i<b.length;i++) {
			if(b[i] == c) {
				break;
			}
		}
		
		byte[] r1 = Arrays.copyOfRange(b, 0, i);
		byte[] r2 = Arrays.copyOfRange(b, i+1, b.length);
		return new byte[][] {r1,r2};		
	}
	
	public static Iterable<byte[]> splitIterable(byte [] b,byte c,int start,int len) {
		return new SpliIterable(b,c,start,len);
	}


	public static Iterator<byte[]> splitIterator(byte [] b,byte c,int start,int len) {
		return new SplitIterator(b,c,start,len);
	}
	
	/*
	 *  =======================================================================================
	 */
	
	static public void printBytes(byte[] x) {
		ByteArray.printBytes(x,x.length);
	}

	static public void printBytes(byte[] x,int size) {
		for(int i=0;i<size;i++) {
			System.out.printf("\\x%x",x[i]);
		}
	}
	
	/*
	 *  =======================================================================================
	 */
	
	static public byte[] trim(byte[] b) {
		if(b.length == 0) return b;
		
		int i=0;
		int j=b.length-1;
		for(;i<b.length & (b[i] == (byte)' ');i++) {}
		for(;j>i & (b[j] == (byte)' ');j--) {  };
		return Arrays.copyOfRange(b, i, j+1);
	}
	
	static public byte[] unquote(byte[] b) {
		if (b.length < 2) return b;
		if (b[0] == (byte) '"' || b[0] == (byte) '\'') {
			if (b[0] == b[b.length-1]) {
				return Arrays.copyOfRange(b, 1, b.length-1);
			} else {
				return b;
			}
		} else {
			return b;
		}
			
	}
}
