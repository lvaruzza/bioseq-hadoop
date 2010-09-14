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

import com.lifetech.hadoop.mapreduce.input.InvalidFastaRecord;
import com.lifetech.hadoop.mapreduce.input.SequenceMaker;

/**
 * Read Fasta sequence
 * 
 */
@Deprecated
public class FastaRecordReader implements RecordReader<LongWritable, Text> {
	
	public static final String START_TOKEN = "start.token";

	private byte[] startToken1 = ">".getBytes();
	private byte[] startToken2 = "\n>".getBytes();

	private long start;
	private long end;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private SequenceMaker seqMaker = new SequenceMaker();	
	boolean endOfFile = false;

	private LongWritable key = new LongWritable();
	private Text value = new Text();

	private JobConf jobConf;
	
	private boolean isQualityFasta = false;
	private boolean colorSpace = false;
	
	/*
	 * TODO: Create a more flexible way to deal with fasta types
	 */
	private void setFastaTypeByExtension(Path path) {
		if (path.getName().endsWith(".qual")) {
			isQualityFasta = true;
		} else {
			isQualityFasta = false;			
			if (path.getName().endsWith(".csfasta")) {
				colorSpace = true;
			} else {
				colorSpace = false;				
			}
		}
	}
	
	
	
	public FastaRecordReader(FileSplit split,JobConf jobConf)
				throws IOException {
		
		this.jobConf = jobConf;

		// open the file and seek to the start of the split
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		setFastaTypeByExtension(file);
		FileSystem fs = file.getFileSystem(jobConf);
		fsin = fs.open(split.getPath());
		fsin.seek(start);				
		
	}
	
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

			if (b==-1) endOfFile=true;
			
			// end of file:
			if (endOfFile && !withinBlock)
				return false;

			// end of sequence:
			if (endOfFile && withinBlock)
				return true;

			// check if we're matching:
			boolean retry;
			do {
				retry = false;
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else {
					// if there is a mismatch, restart the match process
					if (i!=0) retry =true;
					i = 0;
				}
			} while(retry);
			
			// save to buffer:
			if (withinBlock)
				buffer.write(b);
			
			// see if we've passed the stop point:
			if (!withinBlock && i == 0 && fsin.getPos() >= end)
				return false;
		}
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
	public boolean next(LongWritable key, Text value) throws IOException {
		if (fsin.getPos() < end) {
			if (readUntilMatch(startToken1, false)) {
				try {
					//buffer.write(startToken);
					if (readUntilMatch(startToken2, true) || endOfFile) {
						try {
							key.set(fsin.getPos());
							seqMaker.parseBuffer(
										buffer.getData(),
										buffer.getLength(),
										isQualityFasta,
										colorSpace,
										value);
						} catch (InvalidFastaRecord e) {
							throw new RuntimeException(e);
						}
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