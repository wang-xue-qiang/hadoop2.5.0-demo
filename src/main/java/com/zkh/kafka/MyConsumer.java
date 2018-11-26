package com.zkh.kafka;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
/**
 * 
 * @Description:kafka消费者
 * @author wangxueqiang
 * @date 2018年10月31日 上午2:21:30
 *
 */
public class MyConsumer extends Thread{
	private final String topic ;
	private final kafka.javaapi.consumer.ConsumerConnector consumer ;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();
	public MyConsumer(String topic){
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic =topic;
	}
	public static ConsumerConfig  createConsumerConfig(){
		Properties props = new Properties();
		 props.put("zookeeper.connect", "hadoop-senior.ibeifeng.com:2181");
		 props.put("bootstrap.servers", "hadoop-senior.ibeifeng.com:9092");
	     props.put("group.id", "area_order");
	     props.put("enable.auto.commit", "false");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new ConsumerConfig(props);
		
	}
	@Override
	public void run() {
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(topic,new Integer(1));
		Map<String,List<KafkaStream<byte[],byte[]>>>  consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[],byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[],byte[]> it = stream.iterator();
		while(it.hasNext()){
			String msg = new String(it.next().message());
			System.out.println("=================>"+msg);
			queue.add(msg);
		}
	}
	public Queue<String> getQueue() {
		return queue;
	}
	public static void main(String[] args){
		new MyConsumer("myproducer").start();
    }

}
