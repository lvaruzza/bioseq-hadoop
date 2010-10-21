package com.lifetech.hadoop.bioseq;

import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FourBitsEncoderTest {

	private FourBitsEncoder encoder = new FourBitsEncoder(); 

	@Test
	public void testEvenSize() {
		byte[] a = "T012".getBytes();
		byte[] b = encoder.encode(a, 4);
		encoder.printBytes(b,2);
		byte[] c = encoder.decode(b,2);
		System.out.println(new String(c));
		assertEquals(new String(a),new String(c));
	}
	
	@Test
	public void testOddSize() {
		byte[] a = "T0123".getBytes();
		byte[] b = encoder.encode(a, 5);
		encoder.printBytes(b,3);
		byte[] c = encoder.decode(b,3);
		System.out.println(new String(a) + " [" + new String(c) + "|");
		assertEquals(new String(a),new String(c));
	}

	@Test
	public void testSubArrayEncode() {
		byte[] orig = "T01231111".getBytes();
		byte[] a = Arrays.copyOfRange(orig, 5, 9);
		byte[] b = encoder.encode(orig, 5,4);
		encoder.printBytes(b,2);
		System.out.println("");
		
		byte[] c = encoder.decode(b,b.length);
		System.out.println(new String(a) + " [" + new String(c) + "|");
		assertEquals(new String(a),new String(c));
	}

	@Test
	public void testSubArrayDecode() {
		byte[] orig = {0,0,(byte)0x99,(byte)0x99,0};
		encoder.printBytes(orig,orig.length);
		System.out.println();
		
		byte[] c = encoder.decode(orig,2,2);
		System.out.println(new String(c));
		assertEquals("1111",new String(c));
	}
	
}
