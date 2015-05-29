package iie.kafka.demo;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerTest {
    public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: <topic> <events>");
			System.exit(0);
		}
		
		String topic = args[0];
        long events = Long.parseLong(args[1]);//产生数据条数
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.8.103:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
//      props.put("partitioner.class", "example.producer.DefaultPartitioner");
//      props.put("message.send.max.retries", 3);
//      props.put("retry.backoff.ms", 100);
//      props.put("producer.type", "async");

        //创建Producer，依赖于ProducerConfig
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        int index=0;
        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = index +": "+runtime + ",www.example.com," + ip; 
            index++;
            //定义要发送的消息对象
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
//          KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, "key","partKey"  msg);

            //单个或是批量的消息发送到指定分区 
            producer.send(data);            
        }
        //关闭Producer到所有broker的连接
        producer.close();
        System.exit(0);
   }
}
