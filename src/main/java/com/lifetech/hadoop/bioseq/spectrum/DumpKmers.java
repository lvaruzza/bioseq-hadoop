package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


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
			KmerTracking value = new KmerTracking();
			
			while (reader.next(key, value)) {
				Writable[] origs= value.get();

				System.out.printf("%s = %d\n",key,origs.length);
				for(Writable orig: origs) {
					KmerOrig orig0 = (KmerOrig)orig;
					System.out.printf("\t%s(%d)\n",orig0.getReadName(),orig0.getPosition().get());
				}
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}