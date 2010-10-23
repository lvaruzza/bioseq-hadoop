package com.lifetech.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.utils.FastqUtils;


public class SequenceMaker {

	private static byte NEWLINE='\n';
	
	public static int indexOf(byte[] array, byte valueToFind, int startIndex,int stopIndex) {
		if (array == null) {
			return -1;
		}
		if (startIndex < 0) {
			startIndex = 0;
		}
		for (int i = startIndex; i < stopIndex; i++) {
			if (valueToFind == array[i]) {
				return i;
			}
		}
		return -1;
	}

	/*
	 * TODO: Cuidado, esse código só trabalha com \n, não aceita \r\n ou outro terminador de
	 *        linha.
	 */
	public static int indexOfNewline(byte[] array, int startIndex,int stopIndex) {
		return indexOf(array,NEWLINE,startIndex,stopIndex);
	}
	
	private DataOutputBuffer buffer = new DataOutputBuffer();


	public void parseBuffer(byte[] data, int length,
			boolean isQualityFasta,boolean colorSpace,Text result) throws InvalidFastaRecord {
		//DataOutputBuffer buffer = new DataOutputBuffer();
		buffer.reset();
		//out.println(String.format(">1|%s|1<",new String(data,0,length)));
		int start = 0;
		int end = 0;
		
		start = indexOfNewline(data, start,length);
		if (start == -1) {
			throw new InvalidFastaRecord("Missing the header line");
		}
		
		try {			
			buffer.write(data, 0, start);
			buffer.write('\t');
			start++;
			while(true) {
				end = indexOfNewline(data,start,length);
				if (end == -1) break;
				//out.println(String.format("%d %d %d |%s|",start,end,end-start,new String(Arrays.copyOfRange(data, start, end))));
				buffer.write(data,start,end-start);				
				//out.println(String.format("buffer len = %d",buffer.getLength()));
				start = end+1;
			}
			buffer.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		//out.println(String.format("#2|%d %s|2#",buffer.getLength(),new String(buffer.getData(),0,buffer.getLength())));
		result.set(buffer.getData(),0, buffer.getLength());
	}

	public void parseBuffer(byte[] data, int length,
			boolean isQualityFasta,boolean addFirstBase,BioSeqWritable result) throws InvalidFastaRecord {
		//DataOutputBuffer buffer = new DataOutputBuffer();
		buffer.reset();
		
		Text sequence = new Text();
		Text id = new Text();
		
		//out.println(String.format(">1|%s|1<",new String(data,0,length)));
		
		int start = 0;
		int end = 0;
		
		start = indexOfNewline(data, start,length);
		if (start == -1) {
			throw new InvalidFastaRecord("Missing the header line");
		}
		id.set(data,0,start);
		start++;
		
		try {
			buffer.reset();
			while(true) {
				end = indexOfNewline(data,start,length);
				if (end == -1) break;
				//out.println(String.format("%d %d %d |%s|",start,end,end-start,new String(Arrays.copyOfRange(data, start, end))));
				buffer.write(data,start,end-start);				
				//out.println(String.format("buffer len = %d",buffer.getLength()));
				start = end+1;
			}
			buffer.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		sequence.set(buffer.getData(), 0, buffer.getLength());
		
		//out.println(String.format("#2|%d %s|2#",buffer.getLength(),new String(buffer.getData(),0,buffer.getLength())));
		if (isQualityFasta) {
			result.set(id,null,new BytesWritable(FastqUtils.convertPhredQualtityBinary(sequence.toString(),addFirstBase)));
		} else {
			result.set(id,sequence,null);
		}
	}
}
