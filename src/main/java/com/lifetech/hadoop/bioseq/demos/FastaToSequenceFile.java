package com.lifetech.hadoop.bioseq.demos;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import com.lifetech.hadoop.bioseq.BioSeqWritable;
import com.lifetech.hadoop.mapreduce.input.FastaInputFormat;

public class FastaToSequenceFile extends Configured implements Tool {

	public static class CopyMapperWithId extends
			Mapper<LongWritable, BioSeqWritable, Text, BioSeqWritable> {

		public void map(LongWritable key, BioSeqWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(value.getId(), value);
		}
	}
	
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
			
			context.write(key, new BioSeqWritable(
					key,
					seq == null ? null : seq,
					qual == null ? null : qual));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Path fastaPath = new Path(args[0]);
		Path qualPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		Job job = new Job(getConf(), "FastaToFastq");
		
		if (fastaPath.getName().endsWith(".csfasta")) {
			System.out.println("Color Space Fasta");
			job.getConfiguration().set("bioseq.colorSpaceInput", "true");
		}
				
		
		job.setJarByClass(FastaToSequenceFile.class);
		job.setInputFormatClass(FastaInputFormat.class);
		FastaInputFormat.setInputPaths(job, fastaPath,qualPath);

		job.setMapperClass(CopyMapperWithId.class);
		job.setMapOutputValueClass(BioSeqWritable.class);
		job.setMapOutputKeyClass(Text.class);
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzoCodec.class,CompressionCodec.class);

		job.setReducerClass(MergeReducer.class);			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BioSeqWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new FastaToSequenceFile(), args);
		System.exit(ret);
	}
}
