package com.amazonaws.dags.hadoop.examples.join;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.amazonaws.dags.hadoop.examples.join.inputformats.CombinedLogKey;
import com.amazonaws.dags.hadoop.examples.join.inputformats.DBPediaLogInputFormat;
import com.amazonaws.dags.hadoop.examples.join.inputformats.DBPediaLogKey;
import com.amazonaws.dags.hadoop.examples.join.inputformats.DBPediaLogValue;
import com.amazonaws.dags.hadoop.examples.join.inputformats.WikipediaLogInputFormat;
import com.amazonaws.dags.hadoop.examples.join.inputformats.WikipediaLogKey;
import com.amazonaws.dags.hadoop.examples.join.inputformats.WikipediaLogValue;

public class ReduceSideJoinExample {
	public static class WikipediaLogMapper extends Mapper<WikipediaLogKey, WikipediaLogValue, CombinedLogKey, Text> {
		private CombinedLogKey outKey = new CombinedLogKey();
		private Text outValue = new Text();

		@Override
		public void map(WikipediaLogKey key, WikipediaLogValue value, Context context) throws IOException, InterruptedException {
			outKey.setPage(key.getPage());
			outKey.setDataSetNumber(0);
			outValue.set(Long.toString(value.getViews()));

			context.write(outKey, outValue);
		}
	}

	public static class DBPediaCategoryMapper extends Mapper<DBPediaLogKey, DBPediaLogValue, CombinedLogKey, Text> {
		private CombinedLogKey outKey = new CombinedLogKey();
		private Text outValue = new Text();

		@Override
		public void map(DBPediaLogKey key, DBPediaLogValue value, Context context) throws IOException, InterruptedException {
			outKey.setPage(key.getPage());
			outKey.setDataSetNumber(1);
			outValue.set(value.getCategory());

			context.write(outKey, outValue);
		}
	}

	public static class ReduceJoinReducer extends Reducer<CombinedLogKey, Text, NullWritable,Text> {
		private NullWritable outKey = NullWritable.get();
		private Text outValue = new Text();

		@Override
		public void reduce(CombinedLogKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sumViews = 0;

			StringBuilder sb = new StringBuilder();
			for (Text value: values) {
				if (key.getDataSetNumber() == 0) {
					sumViews += Long.valueOf(value.toString()); // views
				} else if (key.getDataSetNumber() == 1) {
					sb.append(value.toString() + "\t"); // category
					sb.append(sumViews);
					outValue.set(sb.toString());
					context.write(outKey, outValue);
					sb.setLength(0);
				}
			}
		}
	}
	
	public static class ReduceJoinGroupByMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outValue = new LongWritable();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = StringUtils.split(value.toString(), '\t');
			outKey.set(values[0]); // Category
			outValue.set(Long.parseLong(values[1])); // Views
			context.write(outKey, outValue);
		}
	}

	public static void main(String[] args) throws Exception {

		// Job1 starts
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf);
		job.setJarByClass(ReduceSideJoinExample.class);
		job.setJobName("MapReduceJoinExample");

		// Configure some Hadoop knobs
		job.setMapSpeculativeExecution(true);
		job.setNumReduceTasks(10);

		job.setReducerClass(ReduceJoinReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(CombinedLogKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setGroupingComparatorClass(CombinedLogGroupingComparator.class);
		job.setPartitionerClass(CombinedLogKeyPartitioner.class);

		Path wikipediaLogs = new Path(otherArgs[0]);
		Path dbpediaCategories = new Path(otherArgs[1]);
		Path intermediateOutput = new Path(otherArgs[2]);

		MultipleInputs.addInputPath(job, wikipediaLogs, WikipediaLogInputFormat.class, WikipediaLogMapper.class);
		MultipleInputs.addInputPath(job, dbpediaCategories, DBPediaLogInputFormat.class, DBPediaCategoryMapper.class);

		FileOutputFormat.setOutputPath(job, intermediateOutput);

		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}

		// Job1 ends
		// Job2 starts
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(ReduceSideJoinExample.class);
		job2.setJobName("MapReduceJoinExample - Second stage");

		// Configure some Hadoop knobs
		job2.setMapSpeculativeExecution(true);
		job2.setNumReduceTasks(3);
		job2.setMapperClass(ReduceJoinGroupByMapper.class);
		job2.setReducerClass(LongSumReducer.class);

		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(LongWritable.class);
		job2.setCombinerClass(LongSumReducer.class);

		Path input = intermediateOutput;
		Path output = new Path(otherArgs[3]);

		FileInputFormat.addInputPath(job2, input);
		FileOutputFormat.setOutputPath(job2, output);
		
		boolean success = job2.waitForCompletion(true);
		FileSystem fs = FileSystem.get(new URI(otherArgs[2]), conf2);
		fs.delete(intermediateOutput, true);
		System.exit(success ? 0 : 1);
		// Job2 ends
	}
}