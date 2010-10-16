package com.lifetech.hadoop.bioseq.demos;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import com.lifetech.hadoop.bioseq.BioSeqWritable;
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

	private static Text empty = new Text("");
	
	public static class MergeReducer extends
			Reducer<Text, BioSeqWritable, Text, BioSeqWritable> {

		public void reduce(Text key, Iterable<BioSeqWritable> values,
				Context context) throws IOException, InterruptedException {
			Text seq = new Text();
			BytesWritable qual = new BytesWritable();
			
			for (BioSeqWritable val : values) {
				if (val.getType() == BioSeqWritable.BioSeqType.SequenceOnly)
					seq.set(val.getSequence());
				else if (val.getType() == BioSeqWritable.BioSeqType.QualityOnly)
					qual.set(val.getQuality());
				else
					throw new RuntimeException(String.format("Invalid SeqType '%s' in sequence '%s'", 
							val.getType().name(),val.getId().toString()));
			}
			
			context.write(empty, new BioSeqWritable(
					key,
					seq == null ? null : seq,
					qual == null ? null : qual));
		}
	}

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Path fastaPath = new Path(args[0]);
		Path qualPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		//FileSystem fs = outputPath.getFileSystem(conf);		
		/*if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}*/

		
		Job job = new Job(getConf(), "FastaToSequenceFile");
		
		if (fastaPath.getName().endsWith(".csfasta")) {
			System.out.println("Color Space Fasta");
			job.getConfiguration().set("bioseq.colorSpaceInput", "true");
		}
				
		
		job.setJarByClass(FastaToFastq.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setMapperClass(CopyMapperWithId.class);

		job.setReducerClass(MergeReducer.class);
		
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(FastqOutputFormat.class);

		FastaInputFormat.setInputPaths(job, fastaPath,qualPath);
		//FastaInputFormat.setInputPaths(job,qualPath);
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
