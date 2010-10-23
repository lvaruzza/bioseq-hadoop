package com.lifetech.utils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class FastqUtils {
	public static char sangerQuality(int qual) {
		if (qual > 93)
			qual=93;
		else if (qual<0)
				qual = 0;
		
		
		return  (char) (qual+33);
	}

	public static Text fastqQuality(BytesWritable quality) {
		int size = quality.getLength();
		byte[] in=quality.getBytes();
		byte[] result = new byte[size];
		
		for(int i=0;i<size;i++) {
			result[i] = (byte) (in[i] + 33);
		}
		return new Text(result);
	}
	
	public static String convertPhredQualtity(String phred,boolean addFirstBase) {
		if (addFirstBase)
			return convertPhredQualtityAddFirstBase(phred);
		else
			return convertPhredQualtityDefault(phred);			
	}
	
	public static String convertPhredQualtityAddFirstBase(String phred) {
		String [] quals=phred.split(" +");
		char[] result = new char[quals.length+1];
		result[0] = sangerQuality(0);
		for(int i=0;i<quals.length;i++) {
			result[i+1] = sangerQuality(Integer.parseInt(quals[i]));
		}
		return new String(result);	
	}
	
	public static String convertPhredQualtityDefault(String phred) {
		String [] quals=phred.split(" +");
		char[] result = new char[quals.length];
		for(int i=0;i<quals.length;i++) {
			result[i] = sangerQuality(Integer.parseInt(quals[i]));
		}
		return new String(result);
	}
	
	
	public static byte[] convertPhredQualtityBinary(String phred,boolean addFirstBase) {
		if (addFirstBase)
			return convertPhredQualtityBinaryAddFirstBase(phred);
		else
			return convertPhredQualtityBaseSpaceBinaryDefault(phred);			
	}
	
	public static byte[] convertPhredQualtityBinaryAddFirstBase(String phred) {
		String [] quals=phred.split(" +");
		byte[] result = new byte[quals.length+1];
		result[0] = 0;
		for(int i=0;i<quals.length;i++) {
			result[i+1] = (byte) Integer.parseInt(quals[i]);
		}
		return result;	
	}
	
	public static byte[] convertPhredQualtityBaseSpaceBinaryDefault(String phred) {
		String [] quals=phred.split(" +");
		byte[] result = new byte[quals.length];
		for(int i=0;i<quals.length;i++) {
			result[i] = (byte) Integer.parseInt(quals[i]);
		}
		return result;
	}

	public static byte[] fastqBinary(String fastqQual) {
		int size = fastqQual.length();
		byte[] qual = fastqQual.getBytes();
		byte[] result = new byte[size];
		
		for(int i=0;i<size;i++) {
			result[i]=(byte)(qual[i]-33);
		}
		return result;
	}
	
}
