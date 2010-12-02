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
import org.apache.hadoop.fs.FileStatus;

import com.lifetech.hadoop.bioseq.FourBitsEncoder;


public class DumpKmers {
	private FourBitsEncoder enc = new FourBitsEncoder(); 
	private FileSystem fs;
	private Configuration conf;
	
	private void dumpFile(Path path) throws IOException {
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			BytesWritable key = new BytesWritable();
			IntWritable value = new IntWritable();
			
			while (reader.next(key, value)) {			
				Text kmer = enc.decode(key); 
				//enc.printBytes(key.getBytes(), key.getLength());
				System.out.printf("%s\t%d\n", kmer.toString(),value.get());
			}
		} finally {
			IOUtils.closeStream(reader);
		}		
	}
	
	private void run(String uri) throws IOException {
		conf = new Configuration();
		fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		if (fs.getFileStatus(path).isDir()) {
			FileStatus[] files = fs.listStatus(path);
			for (FileStatus file : files) {
				dumpFile(file.getPath());
			}
		} else {
			dumpFile(path);
		}
	}
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		new DumpKmers().run(uri);
	}
}