package com.lifetech.hadoop.bioseq;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

abstract public class BioSeqEncoder {
	char[] bases = {'A','C','G','T','N'};
	char[] colors = {'0','1','2','3','.'};

	abstract byte[] encode(byte [] data,int size);	
	abstract byte[] encode(byte [] data,int start,int size);	
	abstract byte[] decode(byte [] data,int size);
	abstract byte[] decode(byte [] data,int start,int size);
	abstract public BytesWritable encode(Text sequence);
	abstract public Text decode(BytesWritable sequence);
}
