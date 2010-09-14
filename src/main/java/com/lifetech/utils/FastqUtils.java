package com.lifetech.utils;

public class FastqUtils {
	public static char sangerQuality(int qual) {
		if (qual > 93)
			qual=93;
		else if (qual<0)
				qual = 0;
		
		
		return  (char) (qual+33);
	}
	
	public static String convertPhredQualtity(String phred,boolean colorSpace) {
		if (colorSpace)
			return convertPhredQualtityColorSpace(phred);
		else
			return convertPhredQualtityBaseSpace(phred);			
	}
	
	public static String convertPhredQualtityColorSpace(String phred) {
		String [] quals=phred.split(" +");
		char[] result = new char[quals.length+1];
		result[0] = sangerQuality(0);
		for(int i=0;i<quals.length;i++) {
			result[i+1] = sangerQuality(Integer.parseInt(quals[i]));
		}
		return new String(result);	
	}
	
	public static String convertPhredQualtityBaseSpace(String phred) {
		String [] quals=phred.split(" +");
		char[] result = new char[quals.length];
		for(int i=0;i<quals.length;i++) {
			result[i] = sangerQuality(Integer.parseInt(quals[i]));
		}
		return new String(result);
	}
}
