package com.lifetech.hadoop.bioseq.spectrum;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Import to Hbase kmers to error correction
 */
public class KmerToHbase extends Configured implements Tool {

	public static class CopyMapper
			extends
			Mapper<BytesWritable, IntWritable, BytesWritable, IntWritable> {

		public void map(BytesWritable key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			
			int filter = context.getConfiguration().getInt("spectrum.hbase.filter", 3);
			if (value.get() < filter) {
				context.write(key, value);
			}
		}
	}	

	public static class TableUploader extends TableReducer<BytesWritable,IntWritable,BytesWritable> {

		public void reduce(BytesWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			long count = values.iterator().next().get();
			byte[] row = Arrays.copyOf(key.getBytes(), key.getLength());
			Put put = new Put(row);
			put.add(Bytes.toBytes("data"), Bytes.toBytes("count"), Bytes.toBytes(count));
			
			context.write(key, put);
		}
	}
	
	private void createTable(String name) throws IOException {
		System.out.printf("Creating Table '%s'\n",name);
		HBaseConfiguration config = new HBaseConfiguration();
		// Create table
		HBaseAdmin admin = new HBaseAdmin(config);
		if (admin.isTableAvailable(name)) {
			System.out.printf("Table '%s' already exists, droping it\n",name);
			admin.disableTable(name);
			admin.deleteTable(name);
		}
		HTableDescriptor htd = new HTableDescriptor("test");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		hcd.setCompressionType(Compression.Algorithm.LZO);
		hcd.setBloomfilter(true);
		htd.addFamily(hcd);
		admin.createTable(htd);		
	}

	private int runMR(String tableName,Path inputPath) throws IOException, InterruptedException, ClassNotFoundException {		
		Job job = new Job(getConf(), "KmerHbaseImport");

		//getConf().setBoolean("keep.failed.task.files", true);
		
		job.setJarByClass(KmerToHbase.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(CopyMapper.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(TableUploader.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		
		return job.waitForCompletion(true) ? 0 : 1;		
	}
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		// Path outputPath = new Path(args[1]);
		String tableName="kmers";
		
		createTable(tableName);
		return 0;
		//return runMR(tableName,inputPath);
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new KmerToHbase(), args);
		System.exit(ret);
	}
}
