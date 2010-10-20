package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class DumpKmers {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Text key = new Text();
			LongWritable value = new LongWritable();
			
			while (reader.next(key, value)) {
				System.out.printf("%s\t%d\n",key,value.get());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}