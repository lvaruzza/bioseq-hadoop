package com.lifetech.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

/**
 * 
 */
public class FastaRecordReader extends RecordReader<LongWritable, BioSeqWritable> {
    private static Logger log = Logger.getLogger(FastaRecordReader.class);
	
	public static final String START_TOKEN = "start.token";

	private byte[] startToken1 = ">".getBytes();
	private byte[] startToken2 = "\n>".getBytes();

	private long start;
	private long end;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private SequenceMaker seqMaker = new SequenceMaker();
	
	boolean endOfFile = false;
	boolean addFirstQuality;
	
	private LongWritable key = new LongWritable();
	private BioSeqWritable value = null;

	public FastaRecordReader() {
		this.addFirstQuality = false;
	}
	
	public FastaRecordReader(boolean addFirstQuality) {
		this.addFirstQuality = addFirstQuality;
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
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public BioSeqWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	private boolean isQualityFasta = false;
	
	
	/*
	 * TODO: Create a more flexible way to deal with fasta types
	 */
	private void setFastaTypeByExtension(Path path) {
		if (path.getName().endsWith(".qual")) {
			isQualityFasta = true;
		} else {
			isQualityFasta = false;			
		}
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
		
		setFastaTypeByExtension(file);
		System.out.println(String.format("FastaInputFormat: file '%s'",file.toString()));
		if (addFirstQuality) {
			System.out.println("\tFix the first quality value");			
		}
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
					if (readUntilMatch(startToken2, true) || endOfFile) {
						try {
							key.set(fsin.getPos());
							value = new BioSeqWritable();
							seqMaker.parseBuffer(buffer.getData(),buffer.getLength(),
										isQualityFasta,addFirstQuality,value);							
						} catch(InvalidFastaRecord e) {
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