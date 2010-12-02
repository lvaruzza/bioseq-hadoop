package com.lifetech.hadoop.bioseq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class TestFourBitsEncoder {

	private FourBitsEncoder encoder = new FourBitsEncoder(); 

	@Test
	public void testEvenSize() {
		byte[] a = "T012".getBytes();
		byte[] b = encoder.encode(a, 4);
		FourBitsEncoder.printBytes(b,2);
		byte[] c = encoder.decode(b,2);
		System.out.println(new String(c));
		assertEquals(new String(a),new String(c));
	}
	
	@Test
	public void testOddSize() {
		byte[] a = "T0123".getBytes();
		byte[] b = encoder.encode(a, 5);
		FourBitsEncoder.printBytes(b,3);
		byte[] c = encoder.decode(b,3);
		System.out.println(new String(a) + " [" + new String(c) + "|");
		assertEquals(new String(a),new String(c));
	}

	@Test
	public void testSubArrayEncodeEven() {
		byte[] orig = "T01231111".getBytes();
		byte[] a = Arrays.copyOfRange(orig, 5, 9);
		byte[] b = encoder.encode(orig, 5,4);
		FourBitsEncoder.printBytes(b,2);
		System.out.println("");
		
		byte[] c = encoder.decode(b,b.length);
		System.out.println(new String(a) + " [" + new String(c) + "|");
		assertEquals(new String(a),new String(c));
	}

	@Test
	public void testSubArrayEncodeOdd() {
		byte[] orig = "T01231111".getBytes();
		byte[] a = Arrays.copyOfRange(orig, 4, 9);
		byte[] b = encoder.encode(orig, 4,5);
		FourBitsEncoder.printBytes(b,3);
		System.out.println("");
		
		byte[] c = encoder.decode(b,b.length);
		System.out.println(new String(a) + " [" + new String(c) + "|");
		assertEquals(new String(a),new String(c));
	}
	
	@Test
	public void testSubArrayDecode() {
		byte[] orig = {0,0,(byte)0x99,(byte)0x99,0};
		FourBitsEncoder.printBytes(orig,orig.length);
		System.out.println();
		
		byte[] c = encoder.decode(orig,2,2);
		System.out.println(new String(c));
		assertEquals("1111",new String(c));
	}

	@Test
	public void testKmers() {
		String s = "T012301230123";
		byte[] orig = s.getBytes();
		int k=5;
		System.out.println(s);
		for(int i=1;i<orig.length-k+1;i++) {
			byte[] kmer=encoder.encode(orig, i,k);
			//System.out.printf("len = %d\n",kmer.length);
			
			for(int j=0;j<i;j++) { System.out.print(' '); };
			byte[] decoded = encoder.decode(kmer, kmer.length);
			System.out.printf("%s\t\t",new String(decoded));
			FourBitsEncoder.printBytes(kmer, kmer.length);
			System.out.println();
			assertEquals(s.substring(i, i+k),new String(decoded));
		}
	}
	
	@Test
	public void testReverseOdd() {
		String s = "T012301230123";
		byte[] f = encoder.encode(s.getBytes());
		byte[] r = encoder.reverse(f);
		byte[] rr = encoder.reverse(r);

		assertTrue(Arrays.equals(f, rr));
	}
	
	@Test
	public void testReverseEven() {
		String s = "T01230123012";
		byte[] f = encoder.encode(s.getBytes());
		byte[] r = encoder.reverse(f);
		byte[] rr = encoder.reverse(r);

		assertTrue(Arrays.equals(f, rr));
	}

	private void runTestComplement(String s) {
		byte[] f = encoder.encode(s.getBytes());
		byte[] c = encoder.complement(f);
		System.out.println(s);
		System.out.println(new String(encoder.decode(c)));
		FourBitsEncoder.printBytes(f);
		System.out.println();
		FourBitsEncoder.printBytes(c);
		System.out.println();
		
		byte[] cc = encoder.complement(c);
		assertTrue(Arrays.equals(f, cc));		
	}
	
	@Test
	public void testComplement() {
		runTestComplement("T01230123012");
		runTestComplement("ACGT");
		runTestComplement("ACGT0123acgt");
		runTestComplement("NNNNNACGN9123N0");
	}
}
