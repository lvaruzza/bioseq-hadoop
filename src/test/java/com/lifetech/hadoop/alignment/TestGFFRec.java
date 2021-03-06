package com.lifetech.hadoop.alignment;

import static java.lang.System.out;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.lifetech.hadoop.alignment.GFFParser;
import com.lifetech.hadoop.alignment.GFFRec;

public class TestGFFRec {
	private GFFParser parser = new GFFParser();
	
	@Test
	public void testGFFParser() {
		String gtfString = "AB000381\tTwinscan\tCDS\t380\t401\t.\t+\t0\tgene_id \"001\"; transcript_id \"001.1\";";
		byte[] buff = gtfString.getBytes();
		GFFRec gff = parser.parse(buff, 0, buff.length);
		
		assertEquals(new Text("AB000381"),gff.getSeqname());
		out.println(gff.getSeqname());

		assertEquals(new Text("Twinscan"),gff.getSource());
		out.println(gff.getSource());

		assertEquals(new Text("CDS"),gff.getFeature());
		out.println(gff.getFeature());

		assertEquals(380L,gff.getStart().get());
		out.println(gff.getStart());

		assertEquals(401L,gff.getEnd().get());
		out.println(gff.getEnd());

		assertEquals(Double.NaN,gff.getScore().get(),0);
		out.println(gff.getScore());
		
		assertEquals('+',(char)gff.getStrand().get());
		out.println((char)gff.getStrand().get());

		assertEquals('0',(char)gff.getFrame().get());
		out.println((char)gff.getFrame().get());
		
		out.println(gff.getProperty(new Text("gene_id")));
		assertEquals(new Text("001"),gff.getProperty(new Text("gene_id")));

		out.println(gff.getProperty(new Text("transcript_id")));
		assertEquals(new Text("001.1"),gff.getProperty(new Text("transcript_id")));
		
		out.println(gff.toString());
		assertEquals("AB000381\tTwinscan\tCDS\t380\t401\t.\t+\t0\tgene_id \"001\"; transcript_id \"001.1\"; ",gff.toString());
	}
}
