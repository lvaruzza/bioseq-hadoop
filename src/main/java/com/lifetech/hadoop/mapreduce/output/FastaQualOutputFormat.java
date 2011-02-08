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
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.utils.PathUtils;

public class FastaQualOutputFormat extends
		FileOutputFormat<NullWritable, BioSeqWritable> {
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

		Path fastaFile = getDefaultWorkFile(job, extension);
		Path qualFile = getDefaultWorkQualFile(job, extension);

		FileSystem fs = fastaFile.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fastaOut = fs.create(fastaFile, false);
			FSDataOutputStream qualOut = fs.create(qualFile, false);
			return new FastaQualRecordWriter(fastaOut, qualOut);
		} else {
			FSDataOutputStream fastaOut = fs.create(fastaFile, false);
			FSDataOutputStream qualOut = fs.create(qualFile, false);
			return new FastaQualRecordWriter(new DataOutputStream(
					codec.createOutputStream(fastaOut)), new DataOutputStream(
					codec.createOutputStream(fastaOut)));
		}
	}

	/**
	 * Get the default path and filename for the output format.
	 * 
	 * @param context
	 *            the task context
	 * @param extension
	 *            an extension to add to the filename
	 * @return a full path $output/_temporary/$taskid/part-[mr]-$id
	 * @throws IOException
	 */
	public Path getDefaultWorkQualFile(TaskAttemptContext context,
			String extension) throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		return new Path(committer.getWorkPath(), getUniqueFile(context, "qual",
				extension));
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context,
			String extension) throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		return new Path(committer.getWorkPath(), getUniqueFile(context, "fasta",
				extension));
	}

}
