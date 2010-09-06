package com.lifetech.hadoop.mapreduce.input;

import static java.lang.System.out;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DataOutputBuffer;


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

	//private DataOutputBuffer buffer = new DataOutputBuffer();

	/*
	 * TODO: Cuidado, esse código só trabalha com \n, não aceita \r\n ou outro terminador de
	 *        linha.
	 */
	public byte[] parseBuffer(byte[] data, int length) throws InvalidFastaRecord {
		DataOutputBuffer buffer = new DataOutputBuffer();
		
		//out.println(String.format(">1|%s|1<",new String(data,0,length)));
		
		int start = 0;
		int end = 0;
		
		start = indexOf(data, NEWLINE, start,length);
		if (start == -1) {
			throw new InvalidFastaRecord("Missing the header line");
		}
		
		try {			
			buffer.write(data, 0, start);
			buffer.write('\t');
			start++;
			while(true) {
				end = indexOf(data,NEWLINE,start,length);
				if (end == -1) break;
				//out.println(String.format("%d %d %d |%s|",start,end,end-start,new String(Arrays.copyOfRange(data, start, end))));
				buffer.write(data,start,end-start);				
				//out.println(String.format("buffer len = %d",buffer.getLength()));
				start = end+1;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		//out.println(String.format("#2|%d %s|2#",buffer.getLength(),new String(buffer.getData(),0,buffer.getLength())));
		
		return Arrays.copyOf(buffer.getData(),buffer.getLength());
	}

}
