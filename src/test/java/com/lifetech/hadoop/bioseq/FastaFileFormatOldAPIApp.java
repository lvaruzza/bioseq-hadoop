package com.lifetech.hadoop.bioseq;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.lifetech.hadoop.streaming.FastaInputFormat;

public class FastaFileFormatOldAPIApp {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(FastaFileFormatOldAPIApp.class);
		conf.setJobName("TestaFastaOldAPI");

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		FileSystem fs = outputPath.getFileSystem(conf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(FastaInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf,inputPath);
		FileOutputFormat.setOutputPath(conf,outputPath);

		JobClient.runJob(conf);
	}
}
