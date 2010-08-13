package com.lifetech.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * XMLRecordReader class to read through a given xml document to output xml
 * blocks as records as specified by the start tag and end tag
 * 
 */
@SuppressWarnings("deprecation")
public class FastaRecordReader implements RecordReader<LongWritable, Text> {

	public static final String START_TOKEN = "start.token";

	private final byte[] startToken;

	private final long start;
	private final long end;
	private final FSDataInputStream fsin;
	private final DataOutputBuffer buffer = new DataOutputBuffer();

	public FastaRecordReader(FileSplit split, JobConf jobConf)
			throws IOException {
		startToken = jobConf.get(START_TOKEN).getBytes("utf-8");

		// open the file and seek to the start of the split
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(jobConf);
		fsin = fs.open(split.getPath());
		fsin.seek(start);
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		if (fsin.getPos() < end) {
			if (readUntilMatch(startToken, false)) {
				try {
					buffer.write(startToken);
					if (readUntilMatch(startToken, true)) {
						Sequence s = new Sequence(buffer.getData());
						key.set(fsin.getPos());
						value.set(s.toString().getBytes(), 0, s.toString().getBytes().length);
						if (fsin.getPos() < end) {
							// unget byte
							fsin.seek(this.getPos() - startToken.length);
						}
						return true;
					}
				} finally {
					buffer.flush();
					buffer.reset();
				}
			}
		}
		return false;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return fsin.getPos();
	}

	@Override
	public void close() throws IOException {
		fsin.close();
	}

	@Override
	public float getProgress() throws IOException {
		return (fsin.getPos() - start) / (float) (end - start);
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock)
			throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();

			// end of file:
			if (b == -1 && !withinBlock)
				return false;

			// end of sequence:
			if (b == -1 && withinBlock)
				return true;

			// check if we're matching:
			if (b == match[i]) {
				i++;
				if (i >= match.length) {
					return true;
				}
			} else {
				i = 0;
			}
			// save to buffer:
			if (withinBlock)
				buffer.write(b);
			
			// see if we've passed the stop point:
			if (!withinBlock && i == 0 && fsin.getPos() >= end)
				return false;
		}
	}
}