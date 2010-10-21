package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.lifetech.hadoop.bioseq.FourBitsEncoder;


public class DumpKmers {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		FourBitsEncoder enc = new FourBitsEncoder(); 
		
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			BytesWritable key = new BytesWritable();
			IntWritable value = new IntWritable();
			
			while (reader.next(key, value)) {			
				Text kmer = enc.decode(key); 
				enc.printBytes(key.getBytes(), key.getLength());
				System.out.printf("\t%s\t%d\n", kmer.toString(),value.get());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}