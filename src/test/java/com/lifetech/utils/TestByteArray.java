package com.lifetech.utils;

import static java.lang.System.out;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import static junit.framework.Assert.*;

public class TestByteArray {

	private static class ToS implements Function<byte[],String> {

		@Override
		public String apply(byte[] b) {
			return new String(b);
		}
		
	}
	
	private void testSplitIterable0(String s,String expected) {
		byte[] b=s.getBytes();
		String r = Iterables.toString(Iterables.transform(ByteArray.splitIterable(b, (byte)' ', 0, b.length),new ToS()));
		out.println(r);
		assertEquals(expected,r);
	}
	
	@Test
	public void testSplitIterable() {
		testSplitIterable0("1 2 3 4 5 6 7 8 9","[1, 2, 3, 4, 5, 6, 7, 8, 9]");
		testSplitIterable0("1 2 ","[1, 2]");
		testSplitIterable0("1","[1]");
		testSplitIterable0(" 1 2 ","[, 1, 2]");
		testSplitIterable0("1 2  ","[1, 2, ]");
		testSplitIterable0("","[]");		
		testSplitIterable0("1024","[1024]");
		testSplitIterable0("1024 13","[1024, 13]");
	}
}
