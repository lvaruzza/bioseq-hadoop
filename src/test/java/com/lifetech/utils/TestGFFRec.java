package com.lifetech.utils;

import org.junit.Test;
import static org.junit.Assert.*;
import static java.lang.System.out;

public class TestGFFRec {
	private GFFParser parser = new GFFParser();
	
	@Test
	public void testGFFParser() {
		String gtfString = "AB000381 Twinscan  CDS          380   401   .   +   0  gene_id \"001\"; transcript_id \"001.1\";";
		byte[] buff = gtfString.getBytes();
		GFFRec gff = parser.parse(buff, 0, buff.length); 
		out.println(gff.toString());
		assertEquals("","");
	}
}
