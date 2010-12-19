package com.lifetech.hadoop.alignment;

import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import com.lifetech.utils.ByteArray;


public class GFFParser {

	public MapWritable readProperties(byte[] buff) {
		System.out.println("|" + (new String(buff)) + "|");
		MapWritable map = new MapWritable();
		for(byte[] x: ByteArray.splitIterable(buff, (byte) ';', 0, buff.length)) {			
			byte[][] pair = ByteArray.split2(ByteArray.trim(x), (byte) ' '); 
			System.out.println(new String(pair[0]) +  ":" + new String(pair[1]));
			map.put(new Text(pair[0]), new Text(ByteArray.unquote(pair[1])));
		}
		
		return map;
	}
	
	public GFFRec parse(Text line) {
		return parse(line.getBytes(),0,line.getLength());
	}
	
	public GFFRec parse(byte [] buff,int buffStart,int buffLen) {
		Iterator<byte[]> it = ByteArray.splitIterator(buff, (byte) '\t', buffStart, buffLen);
		
		Text seqname = new Text(it.next());
		Text source = new Text(it.next());
		Text feature = new Text(it.next());
		LongWritable start = new LongWritable(Long.parseLong(new String(it.next())));
		LongWritable end = new LongWritable(Long.parseLong(new String(it.next())));
		String score0 = new String(it.next());
		DoubleWritable score;
		if (score0.equals(".")) {
			score = new DoubleWritable(Double.NaN);
		} else {
			score = new DoubleWritable(Double.parseDouble(score0));
		}
		
		ByteWritable strand = new ByteWritable(it.next()[0]);
		ByteWritable frame = new ByteWritable(it.next()[0]);
		MapWritable properties = readProperties(ByteArray.trim(it.next()));
		
		return new GFFRec(seqname,source, feature,start,
				end, score, strand,
				frame,properties);
	}
	
}
