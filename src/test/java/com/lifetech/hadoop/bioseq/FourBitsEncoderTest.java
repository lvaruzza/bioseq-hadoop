package com.lifetech.hadoop.bioseq;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FourBitsEncoderTest {

	private FourBitsEncoder encoder = new FourBitsEncoder(); 

	@Test
	public void test1() {
		byte[] a = "T012".getBytes();
		byte[] b = encoder.encode(a, 4);
		encoder.printBytes(b,2);
		byte[] c = encoder.decode(b,2);
		System.out.println(new String(c));
		assertEquals(new String(a),new String(c));
	}
	
	@Test
	public void test2() {
		byte[] a = "T0123".getBytes();
		byte[] b = encoder.encode(a, 5);
		encoder.printBytes(b,3);
		byte[] c = encoder.decode(b,3);
		System.out.println(new String(a) + " [" + new String(c) + "|");
		assertEquals(new String(a),new String(c));
	}
	
}
