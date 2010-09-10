package com.lifetech.utils;

public class FastqUtils {
	public static char sangerQuality(int qual) {
		if (qual > 93)
			qual=93;
		else if (qual<0)
				qual = 0;
		
		
		return  (char) (qual+33);
	}
	public static String convertPhredQualtity(String phred) {
		String [] quals=phred.split(" +");
		char[] result = new char[quals.length];
		for(int i=0;i<quals.length;i++) {
			result[i] = sangerQuality(Integer.parseInt(quals[i]));
		}
		return new String(result);
	}
}
