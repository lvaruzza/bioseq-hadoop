package com.lifetech.hadoop.bioseq.demos;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.bioseq.demos.CopyFasta.CopyMapper;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;
import com.lifetech.hadoop.mapreduce.output.FastqOutputFormat;

public class FastaToFastq implements Tool {

	public static class CopyMapperWithId extends
			Mapper<LongWritable, BioSeqWritable, Text, BioSeqWritable> {

		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(value.getId(), value);
		}
	}

	public static class CopyReducer extends
			Reducer<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void reduce(Text key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {
			BioSeqWritable seq = null;
			BioSeqWritable qual = null;
			
			for (BioSeqWritable val : values) {
				if (val.getType() == BioSeqWritable.BioSeqType.SequenceOnly)
					seq = val;
				else if (val.getType() == BioSeqWritable.BioSeqType.QualityOnly)
					qual = val;
			}
			context.write(key, new BioSeqWritable(key,
									seq.getSequence(),
									qual.getQuality()));
		}
	}

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Path fastaPath = new Path(args[0]);
		Path qualPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		FileSystem fs = outputPath.getFileSystem(conf);
		
		/*if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}*/

		Job job = new Job(getConf(), "FastaToFastq");
		job.setJarByClass(FastaToFastq.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setMapperClass(CopyMapper.class);

		job.setReducerClass(CopyReducer.class);
		
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(LongWritable.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(FastqOutputFormat.class);
		
		FastaInputFormat.setInputPaths(job, fastaPath,qualPath);
		FastqOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	public static void main(String[] args) {
		Tool app = new FastaToFastq();
		app.setConf(new Configuration());
		try {
			app.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
