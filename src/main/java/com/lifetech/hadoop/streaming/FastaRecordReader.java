package com.lifetech.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * XMLRecordReader class to read through a given xml document to output xml
 * blocks as records as specified by the start tag and end tag
 * 
 */
public class FastaRecordReader extends RecordReader<LongWritable, Text> {
	
	public static final String START_TOKEN = "start.token";

	private byte[] startToken1 = ">".getBytes();
	private byte[] startToken2 = "\n>".getBytes();

	private long start;
	private long end;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();


	private LongWritable key = new LongWritable();
	private Text value = new Text();

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

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public void initialize(InputSplit split0, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration jobConf = context.getConfiguration();
		//startToken = jobConf.get(START_TOKEN).getBytes("utf-8");
		
		FileSplit split = (FileSplit) split0;
		
		// open the file and seek to the start of the split
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(jobConf);
		fsin = fs.open(split.getPath());
		fsin.seek(start);		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (fsin.getPos() < end) {
			if (readUntilMatch(startToken1, false)) {
				try {
					//buffer.write(startToken);
					if (readUntilMatch(startToken2, true)) {
						//System.out.print("--------> " );
						//System.out.println(buffer.getData());
						Sequence s = new Sequence(buffer.getData(),buffer.getLength());
						//Sequence s = new Sequence("xxxxxx".getBytes());
						key.set(fsin.getPos());
						value = new Text();						
						value.set(s.toString());						
						if (fsin.getPos() < end) {
							// unget byte
							fsin.seek(this.getPos() - startToken1.length);
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
}