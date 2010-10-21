package com.lifetech.hadoop.bioseq;

abstract public class BioSeqEncoder {
	char[] bases = {'A','C','G','T','N'};
	char[] colors = {'0','1','2','3','.'};

	abstract byte[] encode(byte [] data,int size);	
	abstract byte[] decode(byte [] data,int size);
}
