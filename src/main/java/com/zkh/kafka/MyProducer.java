/**
  	启动Kafka集群
		nohup bin/kafka-server-start.sh config/server.properties & 
	创建topic命令
		bin/kafka-topics.sh --create --zookeeper hadoop-senior.ibeifeng.com:2181 --replication-factor 1 --partitions 1 --topic myproducer
	查看已用topic
		bin/kafka-topics.sh --list --zookeeper hadoop-senior.ibeifeng.com:2181
	生产数据
		bin/kafka-console-producer.sh --broker-list hadoop-senior.ibeifeng.com:9092 --topic myproducer
	消费数据
		bin/kafka-console-consumer.sh --zookeeper hadoop-senior.ibeifeng.com:2181 --topic myproducer --from-beginning
 */
package com.zkh.kafka;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * 		
 * @Description: kafka生产者
 * @author wangxueqiang
 * @date 2018年10月31日 上午2:22:03
 *
 */
public class MyProducer{
	private final static String KAFKA_PRODUCER_TOPIC = "myproducer";
	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private final static String area_id[] = {"1","2","3","4","5"};
	private final static String order_amt[] = {"100.10","200.3","300.6","400.5","1000.8"};
	private final static Random random = new Random();
    @SuppressWarnings("resource")
	public static void main(String[] args) throws  Exception {
        Properties properties = new Properties();
        InputStream in = Producer.class.getClassLoader().getResourceAsStream("myproducer.properties");
        properties.load(in);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        int i =0;
        while(true){
        i++;
        String msg=i+"\t"+order_amt[random.nextInt(5)]+"\t"+formatter.format(new Date())+"\t"+area_id[random.nextInt(5)];
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>(KAFKA_PRODUCER_TOPIC, "1", msg);
        producer.send(producerRecord);
        Thread.sleep(1000);
        
        }
        //producer.close();
    }
}