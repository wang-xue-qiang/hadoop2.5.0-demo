package com.zkh.mr.tohbase;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
	export HBASE_HOME=/opt/modules/hbase-0.98.6-hadoop2
	export HADOOP_HOME=/opt/cdh/hadoop-2.5.0-cdh5.3.6
	HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase mapredcp` $HADOOP_HOME/bin/yarn jar $HADOOP_HOME/jars/product2basic.jar
 */
public class Product2BasicMapReduce extends Configured implements Tool {

	//Mapper class
	public static class ProdcutMapper extends TableMapper<Text, ProductModel> {
		private Text outputKey = new Text();
		private ProductModel outputValue = new ProductModel();
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text,ProductModel>.Context context)
				throws IOException, InterruptedException {
			//获取文本
			String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
			if(null == content){
				System.out.println("数据格式错误:"+content);
				return ;
			}
			Map<String,String> map = transferContent2Map(content);
			if(map.containsKey("p_id")){
				outputKey.set(map.get("p_id"));
			}else{
				System.out.println("数据格式错误:"+content);
				return ;
			}
			if(map.containsKey("price")&&map.containsKey("p_name")){
				outputValue.setId(outputKey.toString());
				outputValue.setName(map.get("p_name"));
				outputValue.setPrice(map.get("price"));
			}else{
				System.out.println("数据格式错误:"+content);
				return ;
			}
			context.write(outputKey, outputValue);
			
			
		}
	}
	//Reducer class
	public static class WriteBasicProcutReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable>{
		@Override
		protected void reduce(
				Text key,
				Iterable<ProductModel> values,
				Reducer<Text, ProductModel, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
				for (ProductModel value : values) {
					ImmutableBytesWritable outputKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
					Put put = new Put(Bytes.toBytes(key.toString()));
					put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
					put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
					put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
					context.write(outputKey, put);
				}

		}
	}
	
	//任务调度
	public int run(String[] args) throws Exception {
		// create job
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		
		// set run job class
		job.setJarByClass(this.getClass());
		
		// set job
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs

		// set input and set mapper
		TableMapReduceUtil.initTableMapperJob(
		  "product",       // input table
		  scan,               // Scan instance to control CF and attribute selection
		  ProdcutMapper.class,     // mapper class
		  Text.class,         // mapper output key
		  ProductModel.class,  // mapper output value
		  job //
		 );
		
		// set reducer and output
		TableMapReduceUtil.initTableReducerJob(
		  "online_product",        // output table
		  WriteBasicProcutReducer.class,    // reducer class
		  job//
		 );
		
		job.setNumReduceTasks(1);   // at least one, adjust as required
		
		// submit job
		boolean isSuccess = job.waitForCompletion(true) ;
		
		
		return isSuccess ? 0 : 1;
	}
	
	
	
	
	//执行
	public static void main(String[] args) throws Exception {
		// get configuration
		Configuration conf = HBaseConfiguration.create();
		// submit job
		int status = ToolRunner.run(conf,new Product2BasicMapReduce(),args) ;
		
		// exit program
		System.exit(status);
	}
	
	
	//转换字符转为map
	public static Map<String,String>  transferContent2Map(String content){
		Map<String,String> map = new HashMap<String,String>();
		int i = 0;
		String key = "";
		StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
		while (tokenizer.hasMoreTokens()) {
			if (++i % 2 == 0) {
				// 当前的值是value
				map.put(key, tokenizer.nextToken());
			} else {
				// 当前的值是key
				key = tokenizer.nextToken();
			}
		}
		return map;
	}

}


