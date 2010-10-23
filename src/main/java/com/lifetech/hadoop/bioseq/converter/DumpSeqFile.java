package com.lifetech.hadoop.bioseq.converter;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class DumpSeqFile {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = new Text();
			Writable value = new BioSeqWritable();
			
			while (reader.next(key, value)) {
				System.out.printf("%s\n",value.toString());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}