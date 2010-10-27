package com.lifetech.hadoop.bioseq;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

abstract public class BioSeqEncoder {
	char[] bases = {'A','C','G','T','N'};
	char[] colors = {'0','1','2','3','.'};

	abstract public byte[] encode(byte [] data,int size);	
	abstract public byte[] encode(byte [] data,int start,int size);	
	
	abstract public byte[] decode(byte [] data,int size);
	abstract public byte[] decode(byte [] data,int start,int size);
	

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
}
