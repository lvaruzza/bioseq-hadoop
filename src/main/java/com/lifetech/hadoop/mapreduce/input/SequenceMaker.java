package com.lifetech.hadoop.mapreduce.input;

import java.io.IOException;

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

	private DataOutputBuffer buffer = new DataOutputBuffer();

	/*
	 * TODO: Cuidado, esse código só trabalha com \n, não aceita \r\n ou outro terminador de
	 *        linha.
	 */
	public byte[] parseBuffer(byte[] data, int length) throws InvalidFastaRecord {

		int start = 0;
		int end = 0;

		start = indexOf(data, NEWLINE, start,length);
		
		if (start == -1) {
			throw new InvalidFastaRecord("Missing the header line");
		}
		try {
			buffer.write(data, 0, start);
			
			do {
				end = indexOf(data,NEWLINE,start,length);
				buffer.write(data,start,end-1);				
				start = end;
			} while(end != -1);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return buffer.getData();
	}

}
