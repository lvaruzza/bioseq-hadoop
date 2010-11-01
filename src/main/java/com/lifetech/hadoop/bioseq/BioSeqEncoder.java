package com.lifetech.hadoop.bioseq;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

abstract public class BioSeqEncoder {
	char[] bases = {'A','C','G','T','N'};
	char[] colors = {'0','1','2','3','.'};

	abstract public byte[] encode(byte [] data,int start,int size);	
	abstract public byte[] decode(byte [] data,int start,int size);
	
	public byte[] encode(byte [] data,int size) {
		return encode(data,0,size);
	}
	
	
	public byte[] decode(byte [] data,int size) {
		return decode(data,0,size);
	}
	

	public byte[] encode(byte [] data) {
		return encode(data,0,data.length);
	}
	
	
	public byte[] decode(byte [] data) {
		return decode(data,0,data.length);
	}
	
	public Text decode(BytesWritable sequence) {
		return new Text(decode(sequence.getBytes(),sequence.getLength()));
	}

	public BytesWritable encode(Text sequence) {
		return new BytesWritable(encode(sequence.getBytes(),sequence.getLength()));
	}
	

	public byte[] encode(String sequence) {
		byte[] bytes = sequence.getBytes();
		return encode(bytes,bytes.length);
	}
	
	abstract public byte[] reverse(byte[] r, int i, int length);
	
	
	public byte[] reverse(byte [] data,int size) {
		return reverse(data,0,size);
	}
	
	public byte[] reverse(byte [] data) {
		return reverse(data,0,data.length);
	}
	
	
}
