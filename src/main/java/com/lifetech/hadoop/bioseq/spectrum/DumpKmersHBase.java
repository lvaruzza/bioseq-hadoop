package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.lifetech.hadoop.bioseq.FourBitsEncoder;

public class DumpKmersHBase {
	public static void main(String[] args) throws IOException {
		Configuration conf = new HBaseConfiguration();
		FourBitsEncoder enc = new FourBitsEncoder();

		HTable table = new HTable("kmers");
		byte[] family = Bytes.toBytes("data");
		byte[] qualifier = Bytes.toBytes("count");
		
		Scan scan = new Scan();
		scan.addColumn(family, qualifier);
		
		ResultScanner scanner = table.getScanner(scan);

		try {
			for (Result r : scanner) {
				FourBitsEncoder.printBytes(r.getRow(), r.getRow().length);
				System.out.print("\t");
				System.out.print(new String(enc.decode(r.getRow(),r.getRow().length)));
				System.out.print("\t");
				System.out.print(Bytes.toLong(r.getValue(family, qualifier)));
				System.out.println();
			}
		} finally {
			scanner.close();
		}
	}
}