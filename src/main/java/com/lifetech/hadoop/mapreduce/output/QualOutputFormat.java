package com.lifetech.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

public class QualOutputFormat extends FileOutputFormat<NullWritable, BioSeqWritable> {
	@Override
	public RecordWriter<NullWritable, BioSeqWritable> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {

		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}

		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new QualRecordWriter(fileOut);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new QualRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
		}
	}
}
