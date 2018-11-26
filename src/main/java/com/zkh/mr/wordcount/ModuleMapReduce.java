package com.zkh.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @Description:  MapReduce模板
 * @author wangxueqiang
 * @date 2018年10月31日 上午2:38:59
 *
 */
public class ModuleMapReduce extends Configured implements Tool {

	// step 1: Map Class
	// public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	public static class ModuleMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			// Nothing
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			// Nothing
		}

	}

	// step 2: Reduce Class
	// public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	public static class ModuleReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Nothing
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Nothing
		}

	}

	// step 3: Driver ,component job
	public int run(String[] args) throws Exception {
		// 1: get confifuration
		Configuration configuration = getConf();

		// 2: create Job
		Job job = Job.getInstance(configuration, //
				this.getClass().getSimpleName());
		// run jar
		job.setJarByClass(this.getClass());

		// 3: set job
		// input -> map -> reduce -> output
		// 3.1: input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);

		// 3.2: map
		job.setMapperClass(ModuleMapper.class);
		// TODO
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//****************************Shuffle*********************************
		// 1) partitioner
//		job.setPartitionerClass(cls);
		// 2) sort
//		job.setSortComparatorClass(cls);
		// 3) optional,combiner
//		job.setCombinerClass(cls);
		// 4) group
//		job.setGroupingComparatorClass(cls);
		
//****************************Shuffle*********************************

		// 3.3: reduce
		job.setReducerClass(ModuleReducer.class);
		// TODO
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set reduce number
//		job.setNumReduceTasks(2);
		
		
		
		// 3.4: output
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);

		// 4: submit job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}
	
	// step 4: run program
	public static void main(String[] args) throws Exception {
		// 1: get confifuration
		Configuration configuration = new Configuration();
		
		//set compress
//		configuration.set("mapreduce.map.output.compress", "true");
//		configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

		// int status = new WordCountMapReduce().run(args);
		int status = ToolRunner.run(configuration,//
				new ModuleMapReduce(),//
				args);

		System.exit(status);
	}

}
