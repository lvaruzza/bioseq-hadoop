/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lifetech.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.lifetech.hadoop.bioseq.BioSeqWritable;

/**
 * Reads records that are delimited by a specifc begin/end tag.
 */
public class FastaInputFormat extends FileInputFormat<LongWritable,BioSeqWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context
				.getConfiguration()).getCodec(file);
		return codec == null;
	}

	@Override
	public RecordReader<LongWritable, BioSeqWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new FastaRecordReader();
	}
}