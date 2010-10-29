package com.lifetech.hadoop.bioseq.spectrum;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.FourBitsEncoder;

public class TestHammingDistance {
	Logger log = Logger.getLogger(TestHammingDistance.class);
	
	private HammingDistance dist = new HammingDistance();
	private BioSeqEncoder enc = new FourBitsEncoder();
	
	private void testStrings(String s1,String s2,double  expected) {
		double  d1 = dist.distance(s1, s2);
		assertEquals(expected,d1,1e-10);
		double  d1e = dist.distance(enc.encode(s1), enc.encode(s2));
		assertEquals(expected,d1e,1e-10);		
	}
	
	@Test
	public void testDistanceZero() {
		testStrings("0123","0123",0.0);
		testStrings("01234","01234",0.0);
	}
	
	@Test
	public void testDistanceOne() {
		testStrings("0122","0123",1.0);
		testStrings("01233","01234",1.0);
		
		testStrings("2123","0123",1.0);
		testStrings("11234","01234",1.0);
		
		testStrings("0023","0123",1.0);
		testStrings("00234","01234",1.0);		
	}

	@Test
	public void testDistanceTwo() {
		testStrings("0213","0123",2.0);
		testStrings("02134","01234",2.0);
		
		testStrings("2120","0123",2.0);
		testStrings("41230","01234",2.0);		
	}
}
