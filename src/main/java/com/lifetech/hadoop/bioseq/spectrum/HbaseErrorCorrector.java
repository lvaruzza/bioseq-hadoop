package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.lifetech.hadoop.bioseq.BioSeqEncoder;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.utils.ByteArray;

public class HbaseErrorCorrector {
	private HTable table;
	private Configuration config;
	private BioSeqEncoder encoder;
	
	public HbaseErrorCorrector(Configuration config,String tableName) throws IOException {		
		this.config = config;
		table = new HTable(tableName);
		encoder = BioSeqWritable.getEncoder();
	}
	
	private static byte[] countColName = Bytes.toBytes("data:count");
	
	public int getKmerCount(byte[] encodedKmer) throws IOException {
		ByteArray.printBytes(encodedKmer, encodedKmer.length);
		Get g = new Get(encodedKmer);
		Result result = table.get(g);	
		if (result.isEmpty()) {
			return 0;
		} else {
			byte[] count = result.getValue(countColName);
			return Bytes.toInt(count);
		}
	}
	
	public void mapKmers(int k,byte[] read,int size) throws IOException {
		System.out.println(new String(Arrays.copyOf(read, size)));
		for(int i=1;i<size-k+1;i++) {
			int count = getKmerCount(encoder.encode(read, i, k));
			for(int j=0;j<i;j++) System.out.print(' '); 
			System.out.print(new String(Arrays.copyOfRange(read, i, i+k)));
			System.out.printf("\t%d\n",count);
		}
	}
	
	public byte[] correctRead(int k,byte [] read,int start,int size,byte [] qual,int qualStart) {
		byte[] r = new byte[size];		
		return r;
	}
}
