/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lifetech.hadoop.bioseq;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * A class that provides a line reader from an input stream.
 */
public class FastaReader {
	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPosn = 0;

	private static final byte CR = '\r';
	private static final byte LF = '\n';
	private static final byte GT = '>';

	/**
	 * Create a line reader that reads from the given stream using the default
	 * buffer-size (64k).
	 * 
	 * @param in
	 *            The input stream
	 * @throws IOException
	 */
	public FastaReader(InputStream in) {
		this(in, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Create a line reader that reads from the given stream using the given
	 * buffer-size.
	 * 
	 * @param in
	 *            The input stream
	 * @param bufferSize
	 *            Size of the read buffer
	 * @throws IOException
	 */
	public FastaReader(InputStream in, int bufferSize) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}

	/**
	 * Create a line reader that reads from the given stream using the
	 * <code>io.file.buffer.sizeIOException</code> specified in the given
	 * <code>Configuration</code>.
	 * 
	 * @param in
	 *            input stream
	 * @param conf
	 *            configuration
	 * @throws IOException
	 */
	public FastaReader(InputStream in, Configuration conf) throws IOException {
		this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
	}

	/**
	 * Close the underlying stream.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		in.close();
	}


	/**
	 * Read one line from the InputStream into the given Text. A line can be
	 * terminated by one of the following: '\n' (LF) , '\r' (CR), or '\r\n'
	 * (CR+LF). EOF also terminates an otherwise unterminated line.
	 * 
	 * @param seq
	 *            the object to store the given line (without newline)
	 * @param maxLineLength
	 *            the maximum number of bytes to store into str; the rest of the
	 *            line is silently discarded.
	 * @param maxBytesToConsume
	 *            the maximum number of bytes to consume in this call. This is
	 *            only a hint, because if the line cross this threshold, we
	 *            allow it to happen. It can overshoot potentially by as much as
	 *            one buffer length.
	 * 
	 * @return the number of bytes read including the (longest) newline found.
	 * 
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	private int readSeq0(Text record, int maxLineLength, int maxBytesToConsume)
			throws IOException {
		/*
		 * We're reading data from in, but the head of the stream may be already
		 * buffered in buffer, so we have several cases: 1. No newline
		 * characters are in the buffer, so we need to copy everything and read
		 * another buffer from the stream. 2. An unambiguously terminated line
		 * is in buffer, so we just copy to str. 3. Ambiguously terminated line
		 * is in buffer, i.e. buffer ends in CR. In this case we copy everything
		 * up to CR to str, but we also need to see what follows CR: if it's LF,
		 * then we need consume LF as well, so next call to readLine will read
		 * from after that. We use a flag prevCharCR to signal if previous
		 * character was CR and, if it happens to be at the end of the buffer,
		 * delay consuming it until we have a chance to look at the char that
		 * follows.
		 */
		record.clear();
		int txtLength = 0; // tracks str.getLength(), as an optimization
		boolean finished = false;
		
		long bytesConsumed = 0;
		do {
			int startPosn = bufferPosn; // starting from where we left off the
										// last time

			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;

				System.out.println("------------- Exausted Buffer -----------------");
				// if (prevCharCR)
				// ++bytesConsumed; //account for CR from previous read

				bufferLength = in.read(buffer);
				if (bufferLength <= 0)
					break; // EOF
			}
			//System.out.println("# First Char = " + (char)buffer[bufferPosn]);
			
			for (; bufferPosn < bufferLength; ++bufferPosn) { // search for
																// newline
				if (buffer[bufferPosn] == GT 
						&& bufferPosn > 0 
						&& (buffer[bufferPosn-1 ] == CR || buffer[bufferPosn-1 ] == LF)) {
					
					++bufferPosn; // at next invocation proceed from following
									// byte
					finished = true;
					break;
				}
			}
			int removeChars = 1; // Remove last > and last new line
			
			if (buffer[bufferPosn-2]==LF) {
				removeChars++;			
				if (buffer[bufferPosn-3]==CR) {
					removeChars++;							
				}
			}
			if (buffer[bufferPosn-2]==CR) {
				removeChars++;
			}
			
			int readLength = bufferPosn - startPosn;

			// if (prevCharCR && newlineLength == 0)
			// --readLength; //CR at the end of the buffer
			bytesConsumed += readLength;

			int appendLength = readLength - removeChars;

			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			
			if (appendLength > 0) {
				record.append(buffer, startPosn, appendLength);
				txtLength += appendLength;
			}
		} while (!finished && bytesConsumed < maxBytesToConsume);

		if (bytesConsumed > (long) Integer.MAX_VALUE)
			throw new IOException("Too many bytes before newline: "
					+ bytesConsumed);
		return (int) bytesConsumed;
	}

	public int readSeq(Text header,Text seq, int maxLineLength, int maxBytesToConsume) throws IOException {
		Text record = new Text();
		int bytesConsumed = readSeq0(record,maxLineLength,maxBytesToConsume);
		
		if (record.charAt(0) == '#') {
			record = new Text();
			bytesConsumed += readSeq0(record,maxLineLength,maxBytesToConsume);
		}
		
		byte[] data = record.getBytes();
		
		int headerEnd = -1;
		
		for(int i=0;i<data.length;i++) {
			if (data[i] == CR  || data[i] == LF) {
				headerEnd = i;
				break;
			}
		}
		if (headerEnd == -1) {
			header.set(data);
			seq.set(new byte[0]);
		} else {
			int seqStart = headerEnd +1;
			header.set(Arrays.copyOfRange(data, 0, headerEnd));
			seq.set(Arrays.copyOfRange(data, seqStart,data.length));
		}
		
		return bytesConsumed;
	}
	
	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @param maxLineLength
	 *            the maximum number of bytes to store into str.
	 * @return the number of bytes read including the newline
	 * @throws IOE
	 *             xception if the underlying stream throws
	 */
	public int readSeq(Text header, Text seq,int maxLineLength) throws IOException {
		return readSeq(header, seq, maxLineLength, Integer.MAX_VALUE);
	}

	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @return the number of bytes read including the newline
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readSeq(Text header,Text seq) throws IOException {
		return readSeq(header,seq, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}

}
