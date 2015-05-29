package iie.kafka.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerTest extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;
	private static String groupId;
	private final int streams;
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: <topic> <thread> <groupId>");
			System.exit(0);
		}
		String topic = args[0];
		int thread = Integer.parseInt(args[1]);
		String groupId = args[2];
		
		ConsumerTest test = new ConsumerTest(topic,thread,groupId);
		test.start();
	}

	public ConsumerTest(String topic,int stream,String group) {
		 // Create the connection to the cluster
		this.topic = topic;
		this.streams= stream;
		groupId = group;
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());
	}

	//定义要连接zookeeper的一些配置信息
	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.8.103:2181/kafka");
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");//zookeeper的session过期时间,如果zk隔指定的时间还没有收到consumer发出的心跳认为consumer死掉，并触发负载均衡
		props.put("zookeeper.sync.time.ms", "200");//ZooKeeper集群中leader和follower之间的同步时间差
		props.put("auto.commit.interval.ms", "1000");//指定多久消费者更新offset到zookeeper中
		return new ConsumerConfig(props);
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic,streams);
		
		/*
		 * 这个API围绕着由KafkaStream实现的迭代器展开，
		 * 每个流由一个线程处理，所以客户端可以在创建的时候通过参数指定想要几个流。
		 * 一个流是多个分区多个broker的合并，但是每个分区的消息只会流向一个流。
		 */
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		
		//这个方法可以得到一个流的列表，每个流都是MessageAndMetadata的迭代，通过MessageAndMetadata可以拿到消息和其他的元数据
		for(int i=0;i<consumerMap.get(topic).size();i++){
			KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(i);
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()){
				System.out.println(new String(it.next().message()));			
			}
		}	
		//提交偏移量
		consumer.commitOffsets();
	}

}